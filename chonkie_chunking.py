from chonkie import RecursiveChunker, RecursiveRules, RecursiveLevel
import re
from typing import List, Dict, Any, Sequence

class AnsibleChonkieLogSplitter:
    """
    Ansible log splitter using Chonkie's RecursiveChunker with custom rules
    for Ansible-specific log boundaries and semantic preservation.
    """
    
    def __init__(self, splitter_type: str = "context", **kwargs):
        """
        Initialize the Ansible log splitter using Chonkie.
        
        Args:
            splitter_type: Type of splitter ('alert', 'context', 'error')
            **kwargs: Additional arguments passed to RecursiveChunker
        """
        self.splitter_type = splitter_type
        
        # Create Ansible-specific recursive rules
        ansible_rules = self._create_ansible_rules(splitter_type)
        
        # Set defaults based on splitter type
        defaults = {
            "alert": {"chunk_size": 500, "min_characters_per_chunk": 50},
            "context": {"chunk_size": 2000, "min_characters_per_chunk": 100}, 
            "error": {"chunk_size": 1000, "min_characters_per_chunk": 75}
        }
        
        # Apply defaults if not overridden
        for key, value in defaults.get(splitter_type, defaults["context"]).items():
            kwargs.setdefault(key, value)
        
        # Initialize Chonkie's RecursiveChunker with our custom rules
        self.chunker = RecursiveChunker(
            rules=ansible_rules,
            tokenizer_or_token_counter="character",  # Use character counting for simplicity
            **kwargs
        )
    
    def _create_ansible_rules(self, splitter_type: str) -> RecursiveRules:
        """Create Ansible-specific RecursiveRules based on splitter type."""
        
        if splitter_type == "alert":
            # Alert mode: Focus on errors and quick boundaries
            levels = [
                # Level 0: RECAP sections (highest priority)
                RecursiveLevel(
                    delimiters=["PLAY RECAP ************"],
                    include_delim="next"
                ),
                # Level 1: Critical errors
                RecursiveLevel(
                    delimiters=[
                        "fatal: [", 
                        "UNREACHABLE!",
                        "FAILED - RETRYING:"
                    ],
                    include_delim="next"
                ),
                # Level 2: Task boundaries
                RecursiveLevel(
                    delimiters=["TASK ["],
                    include_delim="next"
                ),
                # Level 3: Standard fallbacks
                RecursiveLevel(delimiters=["\n\n"]),
                RecursiveLevel(whitespace=True),
            ]
            
        elif splitter_type == "error":
            # Error mode: Focus on failure patterns
            levels = [
                # Level 0: RECAP sections
                RecursiveLevel(
                    delimiters=["PLAY RECAP ************"],
                    include_delim="next"
                ),
                # Level 1: All error patterns
                RecursiveLevel(
                    delimiters=[
                        "fatal: [",
                        "UNREACHABLE!", 
                        "FAILED - RETRYING:",
                        "ERROR!"
                    ],
                    include_delim="next"
                ),
                # Level 2: Task boundaries for context
                RecursiveLevel(
                    delimiters=["TASK ["],
                    include_delim="next"
                ),
                # Level 3: Standard fallbacks
                RecursiveLevel(delimiters=["\n\n"]),
                RecursiveLevel(whitespace=True),
            ]
            
        else:  # context (default)
            # Context mode: Preserve semantic boundaries
            levels = [
                # Level 0: RECAP sections (preserve complete)
                RecursiveLevel(
                    delimiters=["PLAY RECAP ************"],
                    include_delim="next"
                ),
                # Level 1: Major PLAY boundaries
                RecursiveLevel(
                    delimiters=["PLAY ["],
                    include_delim="next"
                ),
                # Level 2: Task boundaries
                RecursiveLevel(
                    delimiters=["TASK [", "RUNNING HANDLER ["],
                    include_delim="next"
                ),
                # Level 3: Host execution patterns
                RecursiveLevel(
                    delimiters=[": ok=", ": changed=", ": failed="],
                    include_delim="prev"
                ),
                # Level 4: Error patterns
                RecursiveLevel(
                    delimiters=[
                        "fatal: [",
                        "UNREACHABLE!",
                        "FAILED - RETRYING:"
                    ],
                    include_delim="next"
                ),
                # Level 5: Timing lines
                RecursiveLevel(
                    delimiters=[" +0000 (", " GMT ("],
                    include_delim="prev"
                ),
                # Level 6: Standard fallbacks
                RecursiveLevel(delimiters=["\n\n"]),
                RecursiveLevel(delimiters=["\n"]),
                RecursiveLevel(whitespace=True),
            ]
        
        return RecursiveRules(levels=levels)
    
    def chunk(self, text: str) -> Sequence:
        """
        Split Ansible log text into chunks using Chonkie's RecursiveChunker.
        
        Args:
            text: Raw Ansible log content
            
        Returns:
            Sequence of RecursiveChunk objects with Ansible-aware boundaries
        """
        # Use Chonkie's chunker to split the text
        chunks = self.chunker.chunk(text)
        return chunks
    
    def split_text(self, text: str) -> List[str]:
        """
        Split text and return just the text content (compatibility method).
        
        Args:
            text: Raw Ansible log content
            
        Returns:
            List of chunk text strings
        """
        chunks = self.chunk(text)
        return [chunk.text for chunk in chunks]


def extract_ansible_metadata_from_chonkie_chunks(chunks) -> List[Dict[str, Any]]:
    """
    Extract Ansible-specific metadata from Chonkie chunks.
    
    Args:
        chunks: Sequence of RecursiveChunk objects from Chonkie
        
    Returns:
        List of metadata dictionaries with extracted information
    """
    metadata_chunks = []
    
    for i, chunk in enumerate(chunks):
        # Convert Chonkie chunk to our metadata format
        chunk_text = chunk.text
        
        metadata = {
            'chunk_index': i,
            'chunk_text': chunk_text,
            'chunk_type': 'standard',
            'chonkie_level': chunk.level,  # Preserve Chonkie's level info
            'chonkie_start': chunk.start_index,
            'chonkie_end': chunk.end_index,
            'chonkie_token_count': chunk.token_count,
            'playbook_name': None,
            'task_names': [],
            'hosts': [],
            'statuses': [],
            'timestamps': [],
            'has_error': False,
            'error_types': [],
            'durations': [],
            'retry_counts': [],
            'host_stats': {},
            'task_timings': []
        }
        
        # Check if this is a RECAP chunk
        if re.search(r'PLAY RECAP \*+', chunk_text, re.MULTILINE):
            metadata['chunk_type'] = 'RECAP'
            metadata['statuses'] = ['SUMMARY']
            
            # Extract host statistics from PLAY RECAP
            host_stat_pattern = r'^([\w\.\-]+)\s*:\s*ok=(\d+)\s+changed=(\d+)\s+unreachable=(\d+)\s+failed=(\d+)\s+skipped=(\d+)\s+rescued=(\d+)\s+ignored=(\d+)'
            host_stats = re.findall(host_stat_pattern, chunk_text, re.MULTILINE)
            
            for host_stat in host_stats:
                hostname, ok, changed, unreachable, failed, skipped, rescued, ignored = host_stat
                metadata['host_stats'][hostname] = {
                    'ok': int(ok),
                    'changed': int(changed), 
                    'unreachable': int(unreachable),
                    'failed': int(failed),
                    'skipped': int(skipped),
                    'rescued': int(rescued),
                    'ignored': int(ignored)
                }
                
                # Determine overall status from host stats
                if int(failed) > 0 or int(unreachable) > 0:
                    metadata['has_error'] = True
                    if int(unreachable) > 0:
                        metadata['error_types'].append('HOST_UNREACHABLE_SUMMARY')
                    if int(failed) > 0:
                        metadata['error_types'].append('TASK_FAILED_SUMMARY')
            
            # Extract all hosts mentioned in RECAP
            metadata['hosts'] = list(metadata['host_stats'].keys())
        
        # Extract task timings (both explicit and implicit TASKS RECAP)
        has_task_timings = False
        
        if 'TASKS RECAP' in chunk_text:
            has_task_timings = True
            timing_pattern = r'^([^-]+?)\s*-+\s*([\d\.]+)s'
            task_timings = re.findall(timing_pattern, chunk_text, re.MULTILINE)
            metadata['chunk_type'] = 'RECAP'
            metadata['statuses'] = ['SUMMARY']
        
        elif '===============================================================================' in chunk_text:
            has_task_timings = True
            parts = chunk_text.split('===============================================================================')
            if len(parts) > 1:
                tasks_section = parts[-1]
                timing_pattern = r'^([^-]+?)\s*-+\s*([\d\.]+)s'
                task_timings = re.findall(timing_pattern, tasks_section, re.MULTILINE)
            else:
                task_timings = []
        
        if has_task_timings:
            for task_name, duration in task_timings:
                metadata['task_timings'].append({
                    'task': task_name.strip(),
                    'duration_seconds': float(duration)
                })
            metadata['chunk_type'] = 'RECAP'
            metadata['statuses'] = ['SUMMARY']
            metadata['task_timings'].sort(key=lambda x: x['duration_seconds'], reverse=True)
        
        # Standard chunk processing (only if not RECAP)
        if metadata['chunk_type'] != 'RECAP':
            # Extract playbook/play name
            playbook_match = re.search(r'PLAY \[(.*?)\]', chunk_text)
            if playbook_match:
                metadata['playbook_name'] = playbook_match.group(1)
            
            # Extract ALL task names
            task_matches = re.findall(r'TASK \[(.*?)\]', chunk_text)
            if task_matches:
                metadata['task_names'] = task_matches
        
        # Extract all host information
        host_patterns = [
            r'(?i)(?:ok|changed|failed|skipping|unreachable|fatal): \[([\w\.\-]+)\]',
            r'(?i)FAILED - RETRYING: \[([\w\.\-]+)\]',
            r'(?i)UNREACHABLE! \[([\w\.\-]+)\]'
        ]
        
        host_matches = []
        for pattern in host_patterns:
            matches = re.findall(pattern, chunk_text)
            host_matches.extend(matches)
        
        if host_matches:
            metadata['hosts'] = list(set(host_matches))
        
        # Extract ALL status and error information
        status_patterns = [
            (r'(?i)FAILED!', 'FAILED', 'TASK_FAILED'),
            (r'(?i)UNREACHABLE!', 'UNREACHABLE', 'HOST_UNREACHABLE'),
            (r'(?i)FAILED - RETRYING', 'RETRYING', 'RETRY_FAILURE'),
            (r'(?i)changed:', 'CHANGED', None),
            (r'(?i)ok:', 'OK', None),
            (r'(?i)skipping:', 'SKIPPING', None),
            (r'(?i)included:', 'INCLUDED', None)
        ]
        
        for pattern, status_name, error_type in status_patterns:
            matches = re.findall(pattern, chunk_text)
            if matches:
                metadata['statuses'].extend([status_name] * len(matches))
                
                if error_type:
                    metadata['has_error'] = True
                    metadata['error_types'].extend([error_type] * len(matches))
        
        # Extract ALL retry counts
        retry_matches = re.findall(r'(?i)FAILED - RETRYING:.*\((\d+) retries left\)', chunk_text)
        if retry_matches:
            metadata['retry_counts'] = [int(count) for count in retry_matches]
        
        # Extract ALL timestamps
        timestamp_patterns = [
            r'([A-Za-z]+ \d+ [A-Za-z]+ \d{4}\s+\d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})',
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})'
        ]
        
        for pattern in timestamp_patterns:
            timestamps = re.findall(pattern, chunk_text)
            metadata['timestamps'].extend(timestamps)
        
        # Extract ALL durations
        duration_matches = re.findall(r'\((\d+:\d{2}:\d{2}\.\d+)\)', chunk_text)
        if duration_matches:
            metadata['durations'] = duration_matches
        
        metadata_chunks.append(metadata)
    
    return metadata_chunks


def create_specialized_chonkie_splitters():
    """
    Create different Ansible log splitters using Chonkie for specific monitoring tasks.
    
    Returns:
        Tuple of (alert_splitter, context_splitter, error_splitter)
    """
    alert_splitter = AnsibleChonkieLogSplitter(
        splitter_type="alert",
        chunk_size=400,
        min_characters_per_chunk=50
    )
    
    context_splitter = AnsibleChonkieLogSplitter(
        splitter_type="context", 
        chunk_size=2500,
        min_characters_per_chunk=100
    )
    
    error_splitter = AnsibleChonkieLogSplitter(
        splitter_type="error",
        chunk_size=800,
        min_characters_per_chunk=75
    )
    
    return alert_splitter, context_splitter, error_splitter


def process_ansible_logs_with_chonkie(log_text: str) -> Dict[str, Any]:
    """
    Process Ansible logs using Chonkie-based splitters with different strategies.
    
    Args:
        log_text: Raw Ansible log content
        
    Returns:
        Dictionary with processed chunks for different use cases
    """
    alert_splitter, context_splitter, error_splitter = create_specialized_chonkie_splitters()
    
    # Split using different strategies
    alert_chunks = alert_splitter.chunk(log_text)
    context_chunks = context_splitter.chunk(log_text)
    error_chunks = error_splitter.chunk(log_text)
    
    # Extract metadata from each chunk type
    return {
        'alert_analysis': {
            'chunks': alert_chunks,
            'metadata': extract_ansible_metadata_from_chonkie_chunks(alert_chunks),
            'use_case': 'real_time_alerting'
        },
        'context_analysis': {
            'chunks': context_chunks, 
            'metadata': extract_ansible_metadata_from_chonkie_chunks(context_chunks),
            'use_case': 'correlation_and_baseline_learning'
        },
        'error_analysis': {
            'chunks': error_chunks,
            'metadata': extract_ansible_metadata_from_chonkie_chunks(error_chunks),
            'use_case': 'failure_pattern_detection'
        }
    }


# Example usage and testing
if __name__ == "__main__":
    # Example usage with Chonkie
    
    # Initialize the splitter
    splitter = AnsibleChonkieLogSplitter(splitter_type="context")

    with open('log_files/job_1434764.txt', 'r') as file:
        sample_log = file.read()
    # Process with Chonkie
    chunks = splitter.chunk(sample_log)
    print(f"Chonkie created {len(chunks)} chunks")
    
    for i, chunk in enumerate(chunks):
        print(f"\n=== Chunk {i+1} (Level {chunk.level}) ===")
        print(f"Start: {chunk.start_index}, End: {chunk.end_index}")
        print(f"Token count: {chunk.token_count}")
        print(f"Text preview: {chunk.text[:100]}...")
    
    # Extract metadata
    metadata = extract_ansible_metadata_from_chonkie_chunks(chunks)
    print(f"\n=== Metadata Example ===")
    for meta in metadata:
        if meta['chunk_type'] == 'RECAP':
            print(f"RECAP chunk found:")
            print(f"  Host stats: {meta['host_stats']}")
            print(f"  Task timings: {len(meta['task_timings'])} tasks")
        elif meta['task_names']:
            print(f"Task chunk with {len(meta['task_names'])} tasks:")
            print(f"  Tasks: {meta['task_names']}")
            print(f"  Statuses: {meta['statuses']}")
    
    # Test complete processing pipeline
    print(f"\n=== Complete Processing Pipeline ===")
    results = process_ansible_logs_with_chonkie(sample_log)
    
    for analysis_type, data in results.items():
        print(f"{analysis_type}: {len(data['chunks'])} chunks")
        recap_count = sum(1 for m in data['metadata'] if m['chunk_type'] == 'RECAP')
        error_count = sum(1 for m in data['metadata'] if m['has_error'])
        print(f"  - RECAP chunks: {recap_count}")
        print(f"  - Error chunks: {error_count}")