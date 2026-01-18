#!/usr/bin/env python3
"""
Python Project Structure Printer

This script prints the directory structure of a Python project in a tree-like format.
It can be used to visualize the organization of a project's files and directories.

Usage: 
    python project_structure.py [path]
    
    - If no path is provided, it will use the current directory.
    - Optional flags:
        --ignore-dirs=dir1,dir2,dir3  # Directories to ignore
        --ignore-files=file1,file2    # Files to ignore
        --max-depth=N                 # Maximum depth to traverse
        --show-hidden                 # Show hidden files and directories
"""

import os
import sys
import argparse
import logging


logging.basicConfig(level=logging.INFO, format="%(message)s")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Print the structure of a Python project."
    )
    parser.add_argument(
        "path",
        nargs="?",
        default=".",
        help="Path to the Python project (default: current directory)",
    )
    parser.add_argument(
        "--ignore-dirs",
        type=str,
        default="__pycache__,venv,.venv,.git,node_modules,build,dist,*.egg-info",
        help="Comma-separated list of directories to ignore",
    )
    parser.add_argument(
        "--ignore-files",
        type=str,
        default="*.pyc,*.pyo,*.pyd,*.so,*.dll,*.class",
        help="Comma-separated list of files to ignore",
    )
    parser.add_argument(
        "--max-depth", type=int, default=None, help="Maximum depth to traverse"
    )
    parser.add_argument(
        "--show-hidden", action="store_true", help="Show hidden files and directories"
    )

    return parser.parse_args()


def should_ignore(name, ignore_list):
    """Check if a file or directory should be ignored based on patterns."""
    for pattern in ignore_list:
        # Simple wildcard handling
        if pattern.startswith("*") and name.endswith(pattern[1:]):
            return True
        elif pattern == name:
            return True
    return False


def print_project_structure(
    path,
    prefix="",
    ignore_dirs=None,
    ignore_files=None,
    max_depth=None,
    current_depth=0,
    show_hidden=False,
):
    """
    Recursively log the directory structure starting from the given path.

    Args:
        path: The directory path to start from
        prefix: String prefix for the current line (used for formatting)
        ignore_dirs: List of directory names to ignore
        ignore_files: List of file patterns to ignore
        max_depth: Maximum depth to traverse (None for unlimited)
        current_depth: Current depth level (used for recursion)
        show_hidden: Whether to show hidden files and directories
    """
    if ignore_dirs is None:
        ignore_dirs = []
    if ignore_files is None:
        ignore_files = []

    # Check if we've reached the maximum depth
    if max_depth is not None and current_depth > max_depth:
        return

    # Get the base directory name
    if current_depth == 0:
        logging.info(os.path.basename(os.path.abspath(path)) or path)

    # Get all items in the directory
    try:
        items = sorted(os.listdir(path))
    except PermissionError:
        logging.info(f"{prefix}├── [Permission Denied]")
        return
    except FileNotFoundError:
        logging.error(f"Error: Directory '{path}' not found.")
        return

    # Filter out hidden files if necessary
    if not show_hidden:
        items = [item for item in items if not item.startswith(".")]

    # Process each item
    for i, item in enumerate(items):
        item_path = os.path.join(path, item)

        # Skip ignored files and directories
        if (os.path.isdir(item_path) and should_ignore(item, ignore_dirs)) or (
            os.path.isfile(item_path) and should_ignore(item, ignore_files)
        ):
            continue

        # Determine if this is the last item to format the tree correctly
        is_last = i == len(items) - 1
        connector = "└── " if is_last else "├── "

        # Print the current item
        logging.info(f"{prefix}{connector}{item}")

        # If item is a directory, recursively process its contents
        if os.path.isdir(item_path):
            # Set up the next level's prefix: either space or vertical line
            next_prefix = prefix + ("    " if is_last else "│   ")
            print_project_structure(
                item_path,
                next_prefix,
                ignore_dirs,
                ignore_files,
                max_depth,
                current_depth + 1,
                show_hidden,
            )


def main():
    """Main function to execute the script."""
    args = parse_arguments()

    # Convert comma-separated strings to lists
    ignore_dirs = [d.strip() for d in args.ignore_dirs.split(",") if d.strip()]
    ignore_files = [f.strip() for f in args.ignore_files.split(",") if f.strip()]

    # Print header
    logging.info("\nPython Project Structure:")
    logging.info("------------------------")

    # Print the project structure
    print_project_structure(
        args.path,
        ignore_dirs=ignore_dirs,
        ignore_files=ignore_files,
        max_depth=args.max_depth,
        show_hidden=args.show_hidden,
    )

    logging.info("\nLegend:")
    logging.info("  ├── Item: Not the last item in a directory")
    logging.info("  └── Item: Last item in a directory")
    logging.info("  │   : Vertical connector for nested items")

if __name__ == "__main__":
    main()
