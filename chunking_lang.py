from langchain.text_splitter import RecursiveCharacterTextSplitter
import re
from typing import List, Dict, Any

class AnsibleLogSplitter(RecursiveCharacterTextSplitter):
    """
    Specialized text splitter for Ansible logs that preserves semantic boundaries
    and maintains context necessary for log monitoring and analysis.
    """
    
    def __init__(self, splitter_type: str = "context", **kwargs):
        """
        Initialize the Ansible log splitter.
        
        Args:
            splitter_type: Type of splitter ('alert', 'context', 'error')
            **kwargs: Additional arguments passed to parent class
        """
        # Define safe Ansible-specific separators without inline flags
        # (langchain wraps patterns in capturing groups, breaking inline flags)
        base_separators = [
            # Major boundaries - PLAY sections 
            r"^PLAY \[.*?\] \*+",
            
            # PLAY RECAP sections  
            r"^PLAY RECAP \*+",
            
            # Task boundaries
            r"^TASK \[.*?\] \*+", 
            r"^RUNNING HANDLER \[.*?\] \*+",
            
            # Host execution blocks
            r"^[A-Za-z0-9\.\-_]+\s*:\s*ok=\d+",
            
            # Error patterns 
            r"^fatal: \[.*?\]: FAILED!",
            r"^FAILED - RETRYING:",
            r"^UNREACHABLE!",
            
            # Timing lines 
            r"^[A-Za-z]+ \d+ [A-Za-z]+ \d{4}\s+\d{2}:\d{2}:\d{2}",
            
            # Standard fallbacks
            "\n\n",
            "\n", 
            " ",
            ""
        ]
        
        # Customize separators based on use case
        if splitter_type == "alert":
            # For real-time alert processing
            separators = [
                r"^fatal: \[.*?\]: FAILED!",
                r"^UNREACHABLE!",  
                r"^FAILED - RETRYING:",
                r"^TASK \[.*?\] \*+",
                "\n\n", "\n", " ", ""
            ]
            
        elif splitter_type == "error": 
            # For error analysis
            separators = [
                r"^fatal: \[.*?\]: FAILED!",
                r"^UNREACHABLE!",
                r"^FAILED - RETRYING:", 
                "\n\n", "\n", " ", ""
            ]
            
        else:  # context (default)
            # For correlation and context analysis
            separators = base_separators
        
        # Set defaults based on splitter type
        defaults = {
            "alert": {"chunk_size": 500, "chunk_overlap": 50},
            "context": {"chunk_size": 2000, "chunk_overlap": 200}, 
            "error": {"chunk_size": 1000, "chunk_overlap": 100}
        }
        
        # Apply defaults if not overridden
        for key, value in defaults.get(splitter_type, defaults["context"]).items():
            kwargs.setdefault(key, value)
        
        super().__init__(
            separators=separators,
            is_separator_regex=True,
            keep_separator="start",  # Put headers at start of chunks for clarity
            **kwargs
        )
    
def extract_ansible_metadata_from_chunks(chunks: List[str]) -> List[Dict[str, Any]]:
    """
    Extract Ansible-specific metadata from text chunks for monitoring system.
    
    Args:
        chunks: List of text chunks from Ansible logs
        
    Returns:
        List of metadata dictionaries with extracted information
    """
    metadata_chunks = []
    
    for i, chunk in enumerate(chunks):
        metadata = {
            'chunk_index': i,
            'chunk_text': chunk,
            'playbook_name': None,
            'task_name': None,
            'hosts': [],
            'status': None,
            'timestamp': None,
            'has_error': False,
            'error_type': None,
            'duration': None,
            'retry_count': 0
        }
        
        # Extract playbook/play name
        playbook_match = re.search(r'PLAY \[(.*?)\]', chunk)
        if playbook_match:
            metadata['playbook_name'] = playbook_match.group(1)
        
        # Extract task name  
        task_match = re.search(r'TASK \[(.*?)\]', chunk)
        if task_match:
            metadata['task_name'] = task_match.group(1)
        
        # Extract all host information (improved with case-insensitive patterns)
        host_patterns = [
            r'(?i)(?:ok|changed|failed|skipping|unreachable|fatal): \[([\w\.\-]+)\]',
            r'(?i)FAILED - RETRYING: \[([\w\.\-]+)\]',
            r'(?i)UNREACHABLE! \[([\w\.\-]+)\]'
        ]
        
        host_matches = []
        for pattern in host_patterns:
            matches = re.findall(pattern, chunk)
            host_matches.extend(matches)
        
        if host_matches:
            metadata['hosts'] = list(set(host_matches))  # Remove duplicates
        
        # Extract status and error information (case-insensitive, error priority)
        if re.search(r'(?i)FAILED!', chunk):
            metadata['status'] = 'FAILED'
            metadata['has_error'] = True
            metadata['error_type'] = 'TASK_FAILED'
        elif re.search(r'(?i)UNREACHABLE!', chunk):
            metadata['status'] = 'UNREACHABLE'
            metadata['has_error'] = True
            metadata['error_type'] = 'HOST_UNREACHABLE'
        elif re.search(r'(?i)FAILED - RETRYING', chunk):
            metadata['status'] = 'RETRYING'
            metadata['has_error'] = True
            metadata['error_type'] = 'RETRY_FAILURE'
            # Extract retry count
            retry_match = re.search(r'(?i)FAILED - RETRYING:.*\((\d+) retries left\)', chunk)
            if retry_match:
                metadata['retry_count'] = int(retry_match.group(1))
        elif re.search(r'(?i)changed:', chunk):
            metadata['status'] = 'CHANGED'
        elif re.search(r'(?i)ok:', chunk):
            metadata['status'] = 'OK'
        elif re.search(r'(?i)skipping:', chunk):
            metadata['status'] = 'SKIPPING'
        
        # Extract timestamp (multiple formats)
        timestamp_patterns = [
            r'([A-Za-z]+ \d+ [A-Za-z]+ \d{4}\s+\d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
        ]
        
        for pattern in timestamp_patterns:
            timestamp_match = re.search(pattern, chunk)
            if timestamp_match:
                metadata['timestamp'] = timestamp_match.group(1)
                break
        
        # Extract duration if available
        duration_match = re.search(r'\((\d+:\d{2}:\d{2}\.\d+)\)', chunk)
        if duration_match:
            metadata['duration'] = duration_match.group(1)
        
        metadata_chunks.append(metadata)
    
    return metadata_chunks


def create_specialized_splitters():
    """
    Create different Ansible log splitters optimized for specific monitoring tasks.
    
    Returns:
        Tuple of (alert_splitter, context_splitter, error_splitter)
    """
    # For real-time alert evaluation - small chunks focusing on status changes
    alert_splitter = AnsibleLogSplitter(
        splitter_type="alert",
        chunk_size=400,
        chunk_overlap=50
    )
    
    # For correlation analysis - larger chunks preserving task context
    context_splitter = AnsibleLogSplitter(
        splitter_type="context", 
        chunk_size=2500,
        chunk_overlap=200
    )
    
    # For error pattern detection - focus on failure sequences
    error_splitter = AnsibleLogSplitter(
        splitter_type="error",
        chunk_size=800,
        chunk_overlap=100
    )
    
    return alert_splitter, context_splitter, error_splitter


def process_ansible_logs_for_monitoring(log_text: str) -> Dict[str, Any]:
    """
    Process Ansible logs for the monitoring system with different chunking strategies.
    
    Args:
        log_text: Raw Ansible log content
        
    Returns:
        Dictionary with processed chunks for different use cases
    """
    alert_splitter, context_splitter, error_splitter = create_specialized_splitters()
    
    # Split using different strategies
    alert_chunks = alert_splitter.split_text(log_text)
    context_chunks = context_splitter.split_text(log_text)
    error_chunks = error_splitter.split_text(log_text)
    
    # Extract metadata from each chunk type
    return {
        'alert_analysis': {
            'chunks': alert_chunks,
            'metadata': extract_ansible_metadata_from_chunks(alert_chunks),
            'use_case': 'real_time_alerting'
        },
        'context_analysis': {
            'chunks': context_chunks, 
            'metadata': extract_ansible_metadata_from_chunks(context_chunks),
            'use_case': 'correlation_and_baseline_learning'
        },
        'error_analysis': {
            'chunks': error_chunks,
            'metadata': extract_ansible_metadata_from_chunks(error_chunks),
            'use_case': 'failure_pattern_detection'
        }
    }


def create_alert_patterns_from_metadata(metadata_chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate alert patterns based on processed chunk metadata (supports PRD requirements).
    
    Args:
        metadata_chunks: List of metadata dictionaries from chunk processing
        
    Returns:
        Dictionary of alert patterns for the monitoring system
    """
    alert_patterns = {
        'unreachable_hosts': {
            'condition': lambda m: m.get('error_type') == 'HOST_UNREACHABLE',
            'severity': 'critical',
            'description': 'Host became unreachable during Ansible execution',
            'natural_language': 'Page me if any host shows UNREACHABLE status'
        },
        'failed_tasks': {
            'condition': lambda m: m.get('error_type') == 'TASK_FAILED',
            'severity': 'high',
            'description': 'Ansible task execution failed',
            'natural_language': 'Alert when Ansible tasks fail'
        },
        'retry_failures': {
            'condition': lambda m: m.get('error_type') == 'RETRY_FAILURE' and m.get('retry_count', 0) < 3,
            'severity': 'medium', 
            'description': 'Task is retrying with few attempts remaining',
            'natural_language': 'Notify if task retries are running low'
        },
        'playbook_duration_anomaly': {
            'condition': lambda m: m.get('playbook_name') and m.get('duration'),
            'severity': 'low',
            'description': 'Playbook execution time exceeds baseline',
            'natural_language': 'Alert if playbook duration exceeds normal baseline'
        }
    }
    
    # Apply patterns to find matching chunks
    triggered_alerts = {}
    for pattern_name, pattern_config in alert_patterns.items():
        matching_chunks = [
            chunk for chunk in metadata_chunks 
            if pattern_config['condition'](chunk)
        ]
        
        if matching_chunks:
            triggered_alerts[pattern_name] = {
                'pattern': pattern_config,
                'triggered_by': matching_chunks,
                'count': len(matching_chunks)
            }
    
    return triggered_alerts

# Example usage for your specific use case
def create_ansible_splitter_for_monitoring():
    """
    Create a splitter optimized for the Ansible Log Monitoring System requirements.
    """
    return AnsibleLogSplitter(
        chunk_size=3500,  # Larger chunks to preserve play context
        chunk_overlap=300  # Good overlap to catch cross-boundary correlations
    )

if __name__ == "__main__":
    # Initialize the splitter
    splitter = AnsibleLogSplitter(splitter_type="context", chunk_size=800, chunk_overlap=100)
    
    with open('log_files/job_1434559.txt', 'r') as file:
        text = file.read()
    
    print(f"Log file size: {len(text)} characters")
    
    # Split the log
    chunks = splitter.split_text(text)
    print(f"Created {len(chunks)} chunks")
    
    # Display results
    for i, chunk in enumerate(chunks[:5]):  # Show first 5 chunks
        print(f"=== Chunk {i+1} ===")
        print(chunk[:200] + "..." if len(chunk) > 200 else chunk)
        print()