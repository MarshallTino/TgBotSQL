#!/usr/bin/env python3
"""
AI-Assistant Ready Context Generator 4.0

Generates a comprehensive project context for AI assistants.
Focuses on your actual code files while ignoring libraries and system files.
Organizes the output by directory structure for better context understanding.
Provides rich architectural and relationship insights for deeper understanding.
"""

import os
import sys
import re
import argparse
from datetime import datetime
import fnmatch
import hashlib
import importlib
import json
import subprocess
from collections import defaultdict

# Project metadata
PROJECT_INFO = {
    "name": "TgBot",
    "description": "Telegram bot for monitoring cryptocurrency messages and tracking token mentions",
    "main_components": ["Telegram Monitor", "Database Storage", "Price Tracker", "Celery Workers"]
}

# -------------- SIZE LIMITS --------------
# Maximum size of any single file to include (bytes)
MAX_FILE_SIZE = 1000 * 1024  # 1000 KB per file
# Maximum number of lines per file
MAX_LINES = 6000  # Reduced to allow more files to fit
# Maximum total size of the final output file (bytes)
MAX_TOTAL_SIZE = 20 * 1024 * 1024  # 20 MB total output limit

# -------------- IGNORE / INCLUDE RULES --------------
IGNORE_PATTERNS = [
    "__pycache__",
    "*.pyc",
    "*site-packages*",
    "node_modules",
    ".git",
    ".pytest_cache",
    ".idea",
    ".vscode",
    ".env",
    "session",
    "volumes",
    "*.jpg",
    "*.png",
    "*.zip",
    "*.gz",
    "*.tar",
    "*.pdf",
    "build",
    "dist",
    "*.egg-info",
    "*.sqlite",
    "*.db",
    ".DS_Store",
    "*.swp",
    "*.swo",
    ".coverage",
    "htmlcov",
    ".tox",
    "*.mo",
    "*.po",
    "*.so",
    "*.o",
    "*.sqlite3",
    "env",
    "venv",
    "*__pycache__*",
    "tg_env",
    # Libraries/modules typically installed and not user-created
    "*celery*",
    "*requests*",
    "*boto*",
    "*pandas*",
    "*numpy*",
    "*pip*"
]

# We primarily want Python files plus a few other meaningful text-based files
INCLUDE_EXTENSIONS = [
    ".py",        # Python source
    ".yml", ".yaml",
    ".sql",
    ".sh",
    ".json",
    ".md",
    ".txt",
    ".conf",
    ".ini",
    ".toml",
    ".env.example"
]

# Core paths we always include if present
CORE_PATHS = [
    "scripts",
    "sql",
    "docker-compose.yml",
    "Dockerfile",
    "Dockerfile.celery",
    "requirements.txt",
    "start_bot.sh",
    "README.md"
]

# Library marker patterns to identify user-created vs imported code
LIBRARY_MARKERS = [
    "site-packages",
    "dist-packages",
    "python/lib",
    "venv/lib",
    "env/lib"
]

def should_ignore(path):
    """
    Check if this path should be ignored based on:
      1) Any of our IGNORE_PATTERNS
      2) File size limit
    """
    # Does this path match an ignore pattern?
    for pattern in IGNORE_PATTERNS:
        if fnmatch.fnmatch(os.path.basename(path), pattern):
            return True
        if fnmatch.fnmatch(path, pattern):
            return True

    # Skip files larger than MAX_FILE_SIZE
    if os.path.isfile(path) and os.path.getsize(path) > MAX_FILE_SIZE:
        return True

    return False

def is_likely_library_code(file_path):
    """
    Determine if a Python file is likely to be library code vs user-created.
    This is a heuristic and may not be 100% accurate.
    """
    # Check if file path contains typical library paths
    for marker in LIBRARY_MARKERS:
        if marker in file_path:
            return True
    
    # Check for common library patterns in content
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read(4096)  # Just read the beginning
            
            # Check for common setup.py patterns
            if 'setup(' in content and 'install_requires' in content:
                return True
                
            # Check for big docstrings typical of libraries
            if content.startswith('"""') and len(content.split('"""')[1]) > 500:
                return True
    except:
        pass
    
    return False

def is_core_file(rel_path):
    """Check if file is in one of the CORE_PATHS."""
    for core_path in CORE_PATHS:
        # If the rel_path itself is exactly 'Dockerfile' or 'docker-compose.yml', etc.
        if rel_path == core_path:
            return True
        # Or if it's within a directory explicitly named in CORE_PATHS
        if rel_path.startswith(core_path + os.sep):
            return True
    return False

def read_file_safely(file_path):
    """
    Read the file content up to MAX_LINES lines.
    If the file has more lines, truncate and show a message.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if len(lines) > MAX_LINES:
                return ''.join(lines[:MAX_LINES]) + (
                    f"\n\n... [File truncated, {len(lines) - MAX_LINES} more lines] ..."
                )
            return ''.join(lines)
    except UnicodeDecodeError:
        return "[Binary or non-text file]"
    except Exception as e:
        return f"[Error reading file: {str(e)}]"

def get_file_type(file_path):
    """Map file extensions to a syntax highlight type for Markdown code fences."""
    ext = os.path.splitext(file_path)[1].lower()
    type_map = {
        '.py': "python",
        '.sh': "bash",
        '.yml': "yaml",
        '.yaml': "yaml",
        '.sql': "sql",
        '.md': "markdown",
        '.json': "json",
        '.txt': "text",
        '.env.example': "text",
        '.conf': "text",
        '.ini': "ini",
        '.toml': "toml"
    }
    return type_map.get(ext, "text")

def get_directory_structure(project_path):
    """
    Generate a directory structure representation with only Python files.
    Returns a dictionary-based tree structure.
    """
    structure = {}
    
    for root, dirs, files in os.walk(project_path):
        # Skip ignored directories
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]
        
        # Get relative path
        rel_path = os.path.relpath(root, project_path)
        if rel_path == '.':
            rel_path = ''
        
        # Get Python files in this directory
        python_files = [f for f in files if f.endswith('.py') and 
                      not should_ignore(os.path.join(root, f))]
        
        if python_files:
            # Create the path in our structure
            path_parts = rel_path.split(os.sep) if rel_path else []
            
            # Navigate to the right spot in the structure
            current = structure
            for part in path_parts:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            # Add files at this level
            current['__files__'] = python_files
    
    return structure

def print_directory_structure(structure, indent=0, path=""):
    """Convert the directory structure to a string representation."""
    output = []
    
    # Print files at this level
    if '__files__' in structure:
        for file in structure['__files__']:
            output.append("  " * indent + "📄 " + file)
        
        # Remove the __files__ key so we don't process it as a directory
        files = structure.pop('__files__')
        
    # Process directories
    for dir_name, contents in sorted(structure.items()):
        output.append("  " * indent + "📁 " + dir_name + "/")
        output.extend(print_directory_structure(contents, indent + 1, 
                     path + ("/" if path else "") + dir_name))
        
    # Put __files__ back if we removed it
    if '__files__' in locals():
        structure['__files__'] = files
        
    return output

def calculate_file_importance(file_path, content):
    """
    Calculate an importance score for a file to prioritize which files to include.
    Higher score = more important for context.
    """
    score = 0
    
    # Core files are always important
    rel_path = os.path.relpath(file_path)
    if is_core_file(rel_path):
        score += 100
        
    # Files with more code are likely more important
    score += min(len(content.split('\n')), 500) / 10
    
    # Files with class definitions or main functions are important
    if re.search(r'class\s+\w+', content):
        score += 20
    if re.search(r'def\s+main', content):
        score += 15
        
    # Files in key directories (adjust as needed for your project)
    if 'scripts/' in file_path:
        score += 10
    if 'core/' in file_path:
        score += 15
    if 'models/' in file_path:
        score += 12
    if 'utils/' in file_path:
        score += 8
        
    # Files that are imported by other files are important
    filename = os.path.basename(file_path)
    module_name = os.path.splitext(filename)[0]
    if f"import {module_name}" in content or f"from {module_name}" in content:
        score += 10
        
    return score

def extract_sql_schemas(project_path):
    """
    Extract CREATE TABLE statements from .sql files.
    """
    schemas = []
    for root, dirs, files in os.walk(project_path):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, project_path)
                if should_ignore(file_path):
                    continue
                content = read_file_safely(file_path)
                table_matches = re.findall(
                    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)',
                    content, re.IGNORECASE
                )
                if table_matches:
                    schemas.append({
                        "file": rel_path,
                        "tables": table_matches
                    })
    return schemas

def analyze_module_dependencies(project_path):
    """
    Analyze Python imports to build a dependency graph.
    Returns a dictionary mapping modules to their dependencies.
    """
    dependencies = defaultdict(set)
    all_modules = set()
    
    for root, dirs, files in os.walk(project_path):
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                if should_ignore(file_path):
                    continue
                    
                rel_path = os.path.relpath(file_path, project_path)
                module_name = os.path.splitext(rel_path.replace(os.sep, '.'))[0]
                all_modules.add(module_name)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    # Look for import statements
                    import_pattern = re.compile(r'^\s*import\s+([\w\.]+)', re.MULTILINE)
                    from_pattern = re.compile(r'^\s*from\s+([\w\.]+)\s+import', re.MULTILINE)
                    
                    for match in import_pattern.finditer(content):
                        dependencies[module_name].add(match.group(1))
                        
                    for match in from_pattern.finditer(content):
                        dependencies[module_name].add(match.group(1))
                except Exception as e:
                    print(f"Error analyzing imports in {file_path}: {e}")
                    
    # Filter dependencies to only include project modules
    project_dependencies = {}
    for module, deps in dependencies.items():
        project_deps = {dep for dep in deps if any(dep.startswith(m) for m in all_modules)}
        if project_deps:
            project_dependencies[module] = project_deps
            
    return project_dependencies

def extract_api_endpoints(project_path):
    """Extract API endpoints defined in the project."""
    endpoints = []
    
    # Look for Flask/FastAPI/Django routes
    route_patterns = [
        # Flask route pattern
        (r'@(?:app|blueprint)\.route\([\'"]([^\'"]+)[\'"]', 'Flask'),
        # FastAPI patterns
        (r'@(?:app|router)\.(?:get|post|put|delete|patch)\([\'"]([^\'"]+)[\'"]', 'FastAPI'),
        # Django URL patterns
        (r'path\([\'"]([^\'"]+)[\'"],\s*([^,)]+)', 'Django'),
        # Celery task patterns
        (r'@app\.task(?:\(.*\))?\s*\n\s*def\s+([^(]+)', 'Celery')
    ]
    
    for root, dirs, files in os.walk(project_path):
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                if should_ignore(file_path):
                    continue
                    
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    rel_path = os.path.relpath(file_path, project_path)
                    
                    for pattern, framework in route_patterns:
                        for match in re.finditer(pattern, content):
                            if framework == 'Django':
                                endpoints.append({
                                    'path': match.group(1),
                                    'handler': match.group(2),
                                    'type': framework,
                                    'file': rel_path
                                })
                            elif framework == 'Celery':
                                endpoints.append({
                                    'task': match.group(1),
                                    'type': framework,
                                    'file': rel_path
                                })
                            else:
                                endpoints.append({
                                    'path': match.group(1),
                                    'type': framework,
                                    'file': rel_path
                                })
                except Exception as e:
                    print(f"Error analyzing endpoints in {file_path}: {e}")
                    
    return endpoints

def extract_environment_variables(project_path):
    """Extract environment variables used in the project."""
    env_vars = set()
    
    # Pattern to match os.getenv() or os.environ.get() calls
    patterns = [
        r'os\.getenv\([\'"](\w+)[\'"]',
        r'os\.environ\.get\([\'"](\w+)[\'"]',
        r'os\.environ\[[\'"](\w+)[\'"]',
        r'ENV\s+(\w+)=',  # Docker ENV syntax
        r'export\s+(\w+)=',  # Bash export syntax
    ]
    
    for root, dirs, files in os.walk(project_path):
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]
        
        for file in files:
            if file.endswith(tuple(INCLUDE_EXTENSIONS)) or file == 'Dockerfile':
                file_path = os.path.join(root, file)
                if should_ignore(file_path):
                    continue
                    
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    for pattern in patterns:
                        for match in re.finditer(pattern, content):
                            env_vars.add(match.group(1))
                except Exception:
                    pass
    
    return sorted(list(env_vars))

def analyze_docker_services(project_path):
    """Extract Docker services and configuration from docker-compose file."""
    services = []
    
    docker_compose_paths = [
        os.path.join(project_path, 'docker-compose.yml'),
        os.path.join(project_path, 'docker-compose.yaml')
    ]
    
    for path in docker_compose_paths:
        if os.path.exists(path):
            try:
                # Try to parse as YAML
                import yaml
                with open(path, 'r') as f:
                    try:
                        data = yaml.safe_load(f)
                        if data and 'services' in data:
                            for service_name, config in data['services'].items():
                                services.append({
                                    'name': service_name,
                                    'image': config.get('image', 'custom'),
                                    'ports': config.get('ports', []),
                                    'depends_on': config.get('depends_on', []),
                                    'environment': config.get('environment', {})
                                })
                    except yaml.YAMLError:
                        pass
            except ImportError:
                # If PyYAML is not available, use regex to extract service names
                try:
                    with open(path, 'r') as f:
                        content = f.read()
                        service_matches = re.findall(r'^\s*(\w+):\s*$', content, re.MULTILINE)
                        for service in service_matches:
                            if service != 'services' and service != 'networks' and service != 'volumes':
                                services.append({'name': service})
                except Exception:
                    pass
    
    return services

def extract_class_info(project_path):
    """Extract information about classes in the project."""
    classes = []
    
    for root, dirs, files in os.walk(project_path):
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                if should_ignore(file_path):
                    continue
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    rel_path = os.path.relpath(file_path, project_path)
                    
                    # Find all class definitions
                    class_pattern = re.compile(r'^\s*class\s+(\w+)(?:\(([^)]*)\))?:', re.MULTILINE)
                    
                    for match in class_pattern.finditer(content):
                        class_name = match.group(1)
                        parent_classes = match.group(2) if match.group(2) else ""
                        
                        # Get class docstring if available
                        class_pos = match.end()
                        docstring = ""
                        docstring_match = re.search(r'^\s+"""(.*?)"""', content[class_pos:], re.DOTALL | re.MULTILINE)
                        if docstring_match:
                            docstring = docstring_match.group(1).strip()
                        
                        # Get methods for this class
                        methods = []
                        method_pattern = re.compile(r'^\s+def\s+(\w+)\(', re.MULTILINE)
                        
                        # Get the class block by finding the next class or EOF
                        next_class = re.search(r'^\s*class\s+\w+', content[class_pos:], re.MULTILINE)
                        if next_class:
                            class_block = content[class_pos:class_pos + next_class.start()]
                        else:
                            class_block = content[class_pos:]
                            
                        for method_match in method_pattern.finditer(class_block):
                            methods.append(method_match.group(1))
                        
                        classes.append({
                            'name': class_name,
                            'file': rel_path,
                            'parents': parent_classes,
                            'docstring': docstring[:100] + "..." if len(docstring) > 100 else docstring,
                            'methods': methods
                        })
                        
                except Exception as e:
                    print(f"Error extracting class info from {file_path}: {e}")
    
    return classes

def generate_context(project_path, output_file):
    """
    Walk through the project, find the relevant files, and dump their
    contents (plus some metadata) into a single Markdown file with enhanced context.
    """
    if not os.path.exists(project_path):
        print(f"Error: Project path '{project_path}' does not exist.")
        sys.exit(1)

    project_path = os.path.abspath(project_path)

    print(f"🔍 Analyzing project directory: {project_path}")
    
    # Keep track of files we are including
    files_to_include = []
    total_content_size = 0

    # Collect relevant files
    print("📂 Finding relevant files...")
    for root, dirs, files in os.walk(project_path):
        # Filter out directories we ignore
        dirs[:] = [d for d in dirs if not should_ignore(os.path.join(root, d))]

        for file in files:
            file_path = os.path.join(root, file)
            if should_ignore(file_path):
                continue

            # We only want certain extensions (plus any "core" files explicitly mentioned)
            rel_path = os.path.relpath(file_path, project_path)
            ext = os.path.splitext(file)[1].lower()
            
            if is_core_file(rel_path) or (ext in INCLUDE_EXTENSIONS):
                # Skip Python files that are likely library code
                if ext == '.py' and is_likely_library_code(file_path):
                    continue
                
                # Read the content
                content = read_file_safely(file_path)
                
                # Calculate importance score for this file
                importance = calculate_file_importance(file_path, content)

                # Track the file
                files_to_include.append({
                    "path": rel_path,
                    "content": content,
                    "type": get_file_type(file_path),
                    "directory": os.path.dirname(rel_path) or "root",
                    "importance": importance,
                    "size": len(content.encode('utf-8'))
                })

    # Sort files by importance (highest first)
    files_to_include.sort(key=lambda x: x["importance"], reverse=True)
    
    # Group files by directory
    files_by_directory = {}
    for fileinfo in files_to_include:
        directory = fileinfo["directory"]
        files_by_directory.setdefault(directory, []).append(fileinfo)

    # Grab any SQL schema info
    sql_schemas = extract_sql_schemas(project_path)
    
    # Analyze module dependencies
    module_dependencies = analyze_module_dependencies(project_path)
    
    # Extract API endpoints
    api_endpoints = extract_api_endpoints(project_path)
    
    # Extract environment variables
    env_vars = extract_environment_variables(project_path)
    
    # Analyze Docker services
    docker_services = analyze_docker_services(project_path)
    
    # Extract class information
    classes = extract_class_info(project_path)
    
    # Generate directory structure with Python files
    directory_structure = get_directory_structure(project_path)
    dir_structure_text = "\n".join(print_directory_structure(directory_structure))

    # -------------- Build the Output --------------
    context_data = []
    
    # Determine output format based on file extension
    output_format = os.path.splitext(output_file)[1].lower()
    is_markdown = output_format == '.md'
    
    # Header section
    if is_markdown:
        context_data.append("# TgBot Project Context")
    else:
        context_data.append("TgBot Project Context")
        context_data.append("=" * 40)
    
    context_data.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Basic project info
    if is_markdown:
        context_data.append("\n## Project Overview")
        context_data.append(f"- **Name**: {PROJECT_INFO['name']}")
        context_data.append(f"- **Description**: {PROJECT_INFO['description']}")
        context_data.append("**Main Components:**")
    else:
        context_data.append("\nPROJECT OVERVIEW")
        context_data.append("-" * 40)
        context_data.append(f"Name: {PROJECT_INFO['name']}")
        context_data.append(f"Description: {PROJECT_INFO['description']}")
        context_data.append("Main Components:")
    
    for component in PROJECT_INFO['main_components']:
        if is_markdown:
            context_data.append(f"- {component}")
        else:
            context_data.append(f"* {component}")
    
    # Add directory structure
    if is_markdown:
        context_data.append("\n## Directory Structure")
    else:
        context_data.append("\nDIRECTORY STRUCTURE")
        context_data.append("-" * 40)
    
    context_data.append(dir_structure_text)

    # Show discovered SQL schemas (if any)
    if sql_schemas:
        if is_markdown:
            context_data.append("\n## Database Schemas")
        else:
            context_data.append("\nDATABASE SCHEMAS")
            context_data.append("-" * 40)
        
        for schema in sql_schemas:
            if is_markdown:
                context_data.append(f"\n### File: {schema['file']}")
                context_data.append("Tables defined:")
            else:
                context_data.append(f"\nFile: {schema['file']}")
                context_data.append("Tables defined:")
            
            for table in schema['tables']:
                if is_markdown:
                    context_data.append(f"- `{table}`")
                else:
                    context_data.append(f"* {table}")

    # Show module dependencies
    if module_dependencies:
        if is_markdown:
            context_data.append("\n## Module Dependencies")
        else:
            context_data.append("\nMODULE DEPENDENCIES")
            context_data.append("-" * 40)
        
        for module, deps in module_dependencies.items():
            if is_markdown:
                context_data.append(f"\n### {module}")
                context_data.append("Depends on:")
            else:
                context_data.append(f"\nModule: {module}")
                context_data.append("Depends on:")
            
            for dep in deps:
                if is_markdown:
                    context_data.append(f"- {dep}")
                else:
                    context_data.append(f"* {dep}")

    # Show API endpoints
    if api_endpoints:
        if is_markdown:
            context_data.append("\n## API Endpoints")
        else:
            context_data.append("\nAPI ENDPOINTS")
            context_data.append("-" * 40)
        
        for endpoint in api_endpoints:
            if endpoint['type'] == 'Celery':
                if is_markdown:
                    context_data.append(f"\n### Task: {endpoint['task']}")
                    context_data.append(f"- **Type**: {endpoint['type']}")
                    context_data.append(f"- **File**: {endpoint['file']}")
                else:
                    context_data.append(f"\nTask: {endpoint['task']}")
                    context_data.append(f"Type: {endpoint['type']}")
                    context_data.append(f"File: {endpoint['file']}")
            else:
                if is_markdown:
                    context_data.append(f"\n### Path: {endpoint['path']}")
                    context_data.append(f"- **Type**: {endpoint['type']}")
                    context_data.append(f"- **File**: {endpoint['file']}")
                    if 'handler' in endpoint:
                        context_data.append(f"- **Handler**: {endpoint['handler']}")
                else:
                    context_data.append(f"\nPath: {endpoint['path']}")
                    context_data.append(f"Type: {endpoint['type']}")
                    context_data.append(f"File: {endpoint['file']}")
                    if 'handler' in endpoint:
                        context_data.append(f"Handler: {endpoint['handler']}")

    # Show environment variables
    if env_vars:
        if is_markdown:
            context_data.append("\n## Environment Variables")
        else:
            context_data.append("\nENVIRONMENT VARIABLES")
            context_data.append("-" * 40)
        
        for var in env_vars:
            if is_markdown:
                context_data.append(f"- {var}")
            else:
                context_data.append(f"* {var}")

    # Docker services section - this is where the error occurs
    if docker_services:
        if is_markdown:
            context_data.append("\n## Docker Services")
        else:
            context_data.append("\nDOCKER SERVICES")
            context_data.append("-" * 40)
        
        for service in docker_services:
            if is_markdown:
                context_data.append(f"\n### {service['name']}")
                context_data.append(f"- **Image**: {service.get('image', 'custom/unspecified')}")
                
                if 'ports' in service:
                    port_str = ', '.join(str(p) for p in service['ports']) if service['ports'] else 'none'
                    context_data.append(f"- **Ports**: {port_str}")
                    
                if 'depends_on' in service:
                    depends_str = ', '.join(service['depends_on']) if service['depends_on'] else 'none'
                    context_data.append(f"- **Depends on**: {depends_str}")
                    
                if 'environment' in service:
                    env_str = json.dumps(service['environment'], indent=2) if service['environment'] else '{}'
                    context_data.append(f"- **Environment**: {env_str}")
            else:
                context_data.append(f"\nService: {service['name']}")
                context_data.append(f"Image: {service.get('image', 'custom/unspecified')}")
                
                if 'ports' in service:
                    port_str = ', '.join(str(p) for p in service['ports']) if service['ports'] else 'none'
                    context_data.append(f"Ports: {port_str}")
                    
                if 'depends_on' in service:
                    depends_str = ', '.join(service['depends_on']) if service['depends_on'] else 'none'
                    context_data.append(f"Depends on: {depends_str}")
                    
                if 'environment' in service:
                    env_str = json.dumps(service['environment'], indent=2) if service['environment'] else '{}'
                    context_data.append(f"Environment: {env_str}")

    # Show class information
    if classes:
        if is_markdown:
            context_data.append("\n## Classes")
        else:
            context_data.append("\nCLASSES")
            context_data.append("-" * 40)
        
        for cls in classes:
            if is_markdown:
                context_data.append(f"\n### {cls['name']}")
                context_data.append(f"- **File**: {cls['file']}")
                context_data.append(f"- **Parents**: {cls['parents']}")
                context_data.append(f"- **Docstring**: {cls['docstring']}")
                context_data.append(f"- **Methods**: {', '.join(cls['methods'])}")
            else:
                context_data.append(f"\nClass: {cls['name']}")
                context_data.append(f"File: {cls['file']}")
                context_data.append(f"Parents: {cls['parents']}")
                context_data.append(f"Docstring: {cls['docstring']}")
                context_data.append(f"Methods: {', '.join(cls['methods'])}")

    # Directory order helps group code logically
    # directories_order helps group code logically
    directories_order = sorted(files_by_directory.keys())

    for directory in directories_order:
        if is_markdown:
            context_data.append(f"\n## {directory} Files")
        else:
            context_data.append(f"\n{directory.upper()} FILES")
            context_data.append("-" * 40)
        
        for fileinfo in files_by_directory[directory]:
            if is_markdown:
                context_data.append(f"\n### {fileinfo['path']}")
                context_data.append(f"```{fileinfo['type']}")
                context_data.append(fileinfo["content"])
                context_data.append("```")
            else:
                context_data.append(f"\nFILE: {fileinfo['path']}")
                context_data.append(f"TYPE: {fileinfo['type']}")
                context_data.append("-" * 80)
                context_data.append(fileinfo["content"])
                context_data.append("-" * 80)

    # Write it all out to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(context_data))

    final_size = os.path.getsize(output_file)
    print(f"\n✅ Context generated: {output_file}")
    print(f"Total files included: {len(files_to_include)}")
    print(f"Final output size: {final_size / (1024*1024):.2f} MB")

def main():
    parser = argparse.ArgumentParser(description="Generate a single-file context from your local Python project.")
    parser.add_argument(
        "project_path",
        nargs="?",
        default=".",
        help="Path to the project directory (defaults to current directory)"
    )
    parser.add_argument(
        "output_file",
        nargs="?",
        default="project_context.md",
        help="Output file name (defaults to 'project_context.md')"
    )
    parser.add_argument(
        "--format", 
        choices=["md", "txt"],
        help="Force output format regardless of extension (md for Markdown, txt for plain text)"
    )
    args = parser.parse_args()
    
    # Handle format override
    output_file = args.output_file
    if args.format:
        base_name = os.path.splitext(output_file)[0]
        output_file = f"{base_name}.{args.format}"
    
    generate_context(args.project_path, output_file)

if __name__ == "__main__":
    main()
