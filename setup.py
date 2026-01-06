#!/usr/bin/env python3
"""
Setup script for Paddle Migration Tool
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed:")
        print(f"  Error: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 7):
        print("✗ Python 3.7 or higher is required")
        return False
    print(f"✓ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def check_node():
    """Check if Node.js is installed"""
    try:
        result = subprocess.run(['node', '--version'], capture_output=True, text=True)
        print(f"✓ Node.js {result.stdout.strip()} detected")
        return True
    except FileNotFoundError:
        print("✗ Node.js is not installed. Please install Node.js from https://nodejs.org/")
        return False

def check_npm():
    """Check if npm is installed"""
    try:
        result = subprocess.run(['npm', '--version'], capture_output=True, text=True)
        print(f"✓ npm {result.stdout.strip()} detected")
        return True
    except FileNotFoundError:
        print("✗ npm is not installed. Please install npm with Node.js")
        return False

def create_venv():
    """Create virtual environment if it doesn't exist"""
    venv_path = 'venv'
    if os.path.exists(venv_path):
        print(f"✓ Virtual environment already exists at {venv_path}")
        return True
    
    print("Creating virtual environment...")
    try:
        subprocess.run([sys.executable, '-m', 'venv', venv_path], check=True, capture_output=True, text=True)
        print(f"✓ Virtual environment created at {venv_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to create virtual environment: {e.stderr}")
        return False

def get_venv_python():
    """Get the Python interpreter path from venv"""
    if sys.platform == 'win32':
        return os.path.join('venv', 'Scripts', 'python.exe')
    else:
        return os.path.join('venv', 'bin', 'python')

def get_venv_pip():
    """Get the pip path from venv"""
    if sys.platform == 'win32':
        return os.path.join('venv', 'Scripts', 'pip.exe')
    else:
        return os.path.join('venv', 'bin', 'pip')

def install_python_dependencies():
    """Install Python dependencies in virtual environment"""
    # Create venv first
    if not create_venv():
        return False
    
    # Get venv pip path
    venv_pip = get_venv_pip()
    
    # Check if venv pip exists
    if not os.path.exists(venv_pip):
        print(f"✗ Virtual environment pip not found at {venv_pip}")
        print("  Try recreating the virtual environment")
        return False
    
    # Install from requirements.txt if it exists, otherwise install individual packages
    if os.path.exists('requirements.txt'):
        if not run_command(f'"{venv_pip}" install -r requirements.txt', "Installing Python dependencies from requirements.txt"):
            return False
    else:
        # Fallback to individual packages
        dependencies = ['pandas', 'flask', 'flask-cors', 'werkzeug', 'requests']
        for dep in dependencies:
            if not run_command(f'"{venv_pip}" install {dep}', f"Installing {dep}"):
                return False
    
    return True

def install_node_dependencies():
    """Install Node.js dependencies"""
    if not run_command("npm install", "Installing Node.js dependencies"):
        return False
    return True

def main():
    """Main setup function"""
    print("🚀 Setting up Paddle Migration Tool...\n")
    
    # Check prerequisites
    if not check_python_version():
        sys.exit(1)
    
    if not check_node():
        sys.exit(1)
    
    if not check_npm():
        sys.exit(1)
    
    print("\n📦 Installing dependencies...\n")
    
    # Install Python dependencies
    if not install_python_dependencies():
        print("\n✗ Failed to install Python dependencies")
        sys.exit(1)
    
    # Install Node.js dependencies
    if not install_node_dependencies():
        print("\n✗ Failed to install Node.js dependencies")
        sys.exit(1)
    
    print("\n✅ Setup completed successfully!")
    print("\n🎉 You can now start the application with:")
    print("   python3 start.py")
    print("\n💡 Note: Dependencies are installed in a virtual environment (venv/)")
    print("   The start script will automatically use the venv Python interpreter.")
    print("\n📖 For more information, see README.md")

if __name__ == "__main__":
    main() 