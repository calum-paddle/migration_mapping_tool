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

def install_python_dependencies():
    """Install Python dependencies"""
    dependencies = ['pandas', 'flask', 'flask-cors', 'werkzeug']
    
    for dep in dependencies:
        if not run_command(f"pip install {dep}", f"Installing {dep}"):
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
    print("   npm start")
    print("\n📖 For more information, see README.md")

if __name__ == "__main__":
    main() 