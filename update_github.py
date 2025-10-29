#py update_github.py
"""
GitHub Auto-Updater Script with Commit Message Prompt
This script stages all changes, asks for a commit message, and pushes to the remote repository.
"""


import subprocess
import os

# ------------------------
# Configuration
# ------------------------
# Path to your local git repo
REPO_PATH = r"D:\Analytics_Engineering\analytics-engineering-bootcamp"

# ------------------------
# Change working directory
# ------------------------
os.chdir(REPO_PATH)
print(f"Working directory set to: {REPO_PATH}\n")

# ------------------------
# Git commands
# ------------------------
def run_git_command(command_list):
    """Run git commands via subprocess"""
    try:
        result = subprocess.run(command_list, capture_output=True, text=True, check=True)
        if result.stdout.strip():
            print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error running {' '.join(command_list)}:\n{e.stderr}\n")

# 1. Check git status
print("📄 Git status:")
run_git_command(["git", "status"])

# 2. Stage all changes
print("➕ Staging all changes...")
run_git_command(["git", "add", "."])

# 3. Prompt for commit message
commit_message = input("✏️  Enter a commit message for this update: ").strip()
if not commit_message:
    print("⚠️  Commit message cannot be empty. Exiting...")
    exit(1)

# 4. Commit changes
print(f"💾 Committing changes: '{commit_message}'")
run_git_command(["git", "commit", "-m", commit_message])

# 5. Push to remote
print("🚀 Pushing to GitHub...")
run_git_command(["git", "push", "origin", "main"])  # replace 'main' with your branch if different

print("✅ Repo updated successfully!")
