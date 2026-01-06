#!/usr/bin/env python3
"""
Startup script for Paddle Migration Tool
Runs both the Flask backend and React frontend
"""

import subprocess
import sys
import time
import threading
import os
import signal
import requests

def check_backend_health():
    """Check if backend is running"""
    try:
        response = requests.get('http://localhost:5001/api/health', timeout=5)
        return response.status_code == 200
    except:
        return False

def get_venv_python():
    """Get the Python interpreter path from venv if it exists"""
    if sys.platform == 'win32':
        venv_python = os.path.join('venv', 'Scripts', 'python.exe')
    else:
        venv_python = os.path.join('venv', 'bin', 'python')
    
    if os.path.exists(venv_python):
        return venv_python
    return None

def start_backend():
    """Start the Flask backend server"""
    print("🚀 Starting Flask backend server...")
    try:
        # Use venv Python if available, otherwise use system Python
        python_exec = get_venv_python() or sys.executable
        if python_exec != sys.executable:
            print(f"   Using virtual environment Python: {python_exec}")
        subprocess.run([python_exec, 'server.py'], check=True)
    except KeyboardInterrupt:
        print("\n🛑 Backend server stopped")
    except Exception as e:
        print(f"❌ Backend server failed to start: {e}")

def start_frontend():
    """Start the React frontend"""
    print("🌐 Starting React frontend...")
    try:
        subprocess.run(['npm', 'start'], check=True)
    except KeyboardInterrupt:
        print("\n🛑 Frontend server stopped")
    except Exception as e:
        print(f"❌ Frontend server failed to start: {e}")

def wait_for_backend():
    """Wait for backend to be ready"""
    print("⏳ Waiting for backend to start...")
    for i in range(30):  # Wait up to 30 seconds
        if check_backend_health():
            print("✅ Backend is ready!")
            return True
        time.sleep(1)
    print("❌ Backend failed to start within 30 seconds")
    return False

def main():
    """Main startup function"""
    print("🎯 Starting Paddle Migration Tool...\n")
    
    # Check if we're in the right directory
    if not os.path.exists('package.json'):
        print("❌ Please run this script from the project root directory")
        sys.exit(1)
    
    # Start backend in a separate thread
    backend_thread = threading.Thread(target=start_backend, daemon=True)
    backend_thread.start()
    
    # Wait for backend to be ready
    if not wait_for_backend():
        print("❌ Failed to start backend")
        sys.exit(1)
    
    # Start frontend
    print("\n🌐 Starting frontend...")
    start_frontend()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        sys.exit(0) 