"""directory imports. help pytest find the challenge folder."""
import os
import sys

# obtain the absolute path to the directory this file is contained in
current_file_dir = os.path.abspath(os.path.dirname(__file__))

# and add it to the PYTHONPATH in order to make the challenge folder
# and its submodules visible to pytest
challenge_dir = os.path.join(current_file_dir, "challenge")
sys.path.append(current_file_dir)
