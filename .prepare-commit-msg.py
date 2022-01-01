#!/usr/bin/python
"""
This is a prepare-commit-msg hook to prepend labels based on modified file paths for use with the trader.
Copy this file to $GITREPOSITORY/.git/hooks/prepare-commit-msg
and mark it executable.
It will prepend [<label>] to your commit message.
"""
import subprocess
import sys


def get_story():
    """Return the story id number associated with the current branch"""
    branchname = subprocess.check_output(["git", "status", "--porcelain"])
    # This looks like: refs/heads/12345678/my-cool-feature
    args = branchname.split(' ')
    if len(args) != 2:
        raise ValueError("Expected format [status] [file]")
    story = int(branchname.split('/')[2])
    return story


def prepend_commit_msg(text):
    """Prepend the commit message with `text`"""
    msgfile = sys.argv[1]

    with open(msgfile) as f:
        contents = f.read()

    with open(msgfile, 'w') as f:
        # Don't append if it's already there
        if not contents.startswith(text):
            f.write(text)
        f.write(contents)


def main():
    # Fail silently
    try:
        story = get_story()
        header = "[#%d] " % story
        prepend_commit_msg(header)
    except:
        pass

if __name__ == '__main__':
    main()
