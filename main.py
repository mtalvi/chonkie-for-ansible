
import langchain
from langchain_text_splitters import RecursiveCharacterTextSplitter
import re


# Read the log file content
with open('log_files/job_1434559.txt', 'r') as file:
    text = file.read()


'''

ANSIBLE_SEPARATORS = [
    r"^TASK \[.*?\].*$",                  # task headers
    r"^PLAY \[.*?\].*$",                  # play headers
    r"^RUNNING HANDLER \[.*?\].*$",       # handlers
    r"^(PLAY RECAP|TASKS RECAP).*$",      # recaps
    r"^fatal: \[.*?\].*$",                # hard errors
    r"^FAILED - RETRYING:.*$",            # retry lines
    r"^\w+\s+\d{1,2}\s+\w+\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+[+-]\d{4}.*\*+\s*$",      # timestamp+asterisks timing lines
    r"\n\n", r"\n", r" ", r"",                # fallbacks
]


splitter = RecursiveCharacterTextSplitter(
    chunk_size=1200,            # logs are verbose;
    chunk_overlap=120,          # ~10%
    separators=ANSIBLE_SEPARATORS,
    is_separator_regex=True,
    keep_separator="start",
)
chunks = splitter.split_text(text)  # list[str]

print(len(chunks))

for i, chunk in enumerate(chunks):
    print(f"Chunk {i+1}:")
    print(chunk)
    print("\n" + "="*100 + "\n")
    if i == 10:
        break

'''

from langchain.text_splitter import RecursiveCharacterTextSplitter
import re

class AnsibleLogSplitter(RecursiveCharacterTextSplitter):
    """Custom text splitter optimized for Ansible logs"""
    
    def __init__(self, **kwargs):
        # Ansible-specific separators that respect log structure
        ansible_separators = [
            # Split by PLAY sections (major boundaries)
            r"\nPLAY \[.*?\] \*+\n",
            
            # Split by TASK sections (individual tasks)
            r"\nTASK \[.*?\] \*+\n",
            
            # Split by PLAY RECAP (end of playbook)
            r"\nPLAY RECAP \*+\n",
            
            # Split by host execution blocks
            r"\n[A-Za-z0-9\.\-_]+\s*:\s*ok=\d+",
            
            # Split by timing lines (tasks with timestamps)
            r"\n[A-Za-z]+ \d+ [A-Za-z]+ \d{4}\s+\d{2}:\d{2}:\d{2}",
            
            # Fall back to standard separators
            "\n\n",
            "\n",
            " ",
            ""
        ]
        
        super().__init__(
            separators=ansible_separators,
            is_separator_regex=True,
            **kwargs
        )

# Usage examples for your monitoring system
def create_ansible_splitters():
    """Create different splitters for different use cases"""
    
    # For alert rule processing - smaller chunks
    alert_splitter = AnsibleLogSplitter(
        chunk_size=500,
        chunk_overlap=50
    )
    
    # For context analysis - larger chunks preserving task boundaries
    context_splitter = AnsibleLogSplitter(
        chunk_size=2000,
        chunk_overlap=100
    )
    
    # For error analysis - focus on failed tasks
    error_splitter = RecursiveCharacterTextSplitter(
        separators=[
            r"\nfatal: \[.*?\]: FAILED!",
            r"\nERROR!",
            r"\nFAILED - RETRYING:",
            r"\nUNREACHABLE!",
            "\n\n",
            "\n"
        ],
        is_separator_regex=True,
        chunk_size=1000,
        chunk_overlap=50
    )
    
    return alert_splitter, context_splitter, error_splitter

# For your NLP processing pipeline
def process_ansible_logs_for_alerting(log_text: str):
    """Process Ansible logs for the alert creation system"""
    
    alert_splitter, context_splitter, error_splitter = create_ansible_splitters()
    
    # Split for different purposes
    alert_chunks = alert_splitter.split_text(log_text)
    context_chunks = context_splitter.split_text(log_text)
    error_chunks = error_splitter.split_text(log_text)
    
    return {
        'alert_chunks': alert_chunks,      # For real-time alert evaluation
        'context_chunks': context_chunks,  # For correlation analysis
        'error_chunks': error_chunks       # For failure pattern detection
    }

# Integration with your Ansible metadata extraction
def extract_ansible_metadata_from_chunks(chunks):
    """Extract Ansible-specific metadata from text chunks"""
    
    metadata_chunks = []
    
    for chunk in chunks:
        metadata = {
            'chunk_text': chunk,
            'playbook_name': None,
            'task_name': None,
            'host': None,
            'status': None,
            'timestamp': None,
            'has_error': False
        }
        
        # Extract playbook name
        playbook_match = re.search(r'PLAY \[(.*?)\]', chunk)
        if playbook_match:
            metadata['playbook_name'] = playbook_match.group(1)
        
        # Extract task name
        task_match = re.search(r'TASK \[(.*?)\]', chunk)
        if task_match:
            metadata['task_name'] = task_match.group(1)
        
        # Extract host information
        host_match = re.search(r'^([a-zA-Z0-9\.\-_]+)\s*:', chunk, re.MULTILINE)
        if host_match:
            metadata['host'] = host_match.group(1)
        
        # Extract status
        if 'FAILED!' in chunk:
            metadata['status'] = 'FAILED'
            metadata['has_error'] = True
        elif 'UNREACHABLE!' in chunk:
            metadata['status'] = 'UNREACHABLE'
            metadata['has_error'] = True
        elif 'changed:' in chunk:
            metadata['status'] = 'CHANGED'
        elif 'ok:' in chunk:
            metadata['status'] = 'OK'
        elif 'skipping:' in chunk:
            metadata['status'] = 'SKIPPING'
        
        # Extract timestamp
        timestamp_match = re.search(r'([A-Za-z]+ \d+ [A-Za-z]+ \d{4}\s+\d{2}:\d{2}:\d{2})', chunk)
        if timestamp_match:
            metadata['timestamp'] = timestamp_match.group(1)
        
        metadata_chunks.append(metadata)
    
    return metadata_chunks

# Example usage for your PRD requirements
def demo_with_your_logs():
    """Demonstrate processing with your example logs"""
    
    # Using your first log (destroy operation)
    sample_log = """
PLAY [Step 0000 Setup runtime] *************************************************

TASK [debug] *******************************************************************
Friday 18 July 2025  21:02:24 +0000 (0:00:00.011)       0:00:00.011 *********** 
skipping: [localhost]

TASK [Check if k8s interpreter venv is installed] ******************************
Friday 18 July 2025  21:02:54 +0000 (0:00:00.024)       0:00:29.650 *********** 
fatal: [bastion.25cj7.internal]: UNREACHABLE! => {"changed": false, "msg": "Failed to connect to the host via ssh"}
    """
    
    splitter = AnsibleLogSplitter(chunk_size=300, chunk_overlap=50)
    chunks = splitter.split_text(text)
    
    # Process for your alert system
    processed_chunks = extract_ansible_metadata_from_chunks(chunks)
    
    for chunk_data in processed_chunks:
        print(f"Host: {chunk_data['host']}")
        print(f"Status: {chunk_data['status']}")
        print(f"Has Error: {chunk_data['has_error']}")
        print(f"Timestamp: {chunk_data['timestamp']}")
        print("---")

# For your alert rule creation (from PRD)
def create_alert_rules_from_chunks():
    """Generate alert rules based on processed chunks"""
    
    # This aligns with your PRD requirement:
    # "Page me if any Ansible job shows 'connection refused' errors"
    alert_patterns = {
        'unreachable_hosts': {
            'pattern': r'UNREACHABLE!',
            'severity': 'critical',
            'description': 'Host became unreachable during playbook execution'
        },
        'failed_tasks': {
            'pattern': r'fatal: .*?: FAILED!',
            'severity': 'high', 
            'description': 'Task execution failed'
        },
        'connection_errors': {
            'pattern': r'Failed to connect.*ssh',
            'severity': 'high',
            'description': 'SSH connection failure'
        }
    }
    
    return alert_patterns

if __name__ == "__main__":
    demo_with_your_logs()