#!/bin/bash

set -e

README_PATH="README.md"
DIARY_FOLDER="docs/diary"
DIARY_SECTION_MARKER="## Diary"

# Read the current README content
README_CONTENT=$(cat "$README_PATH")

# Find the Diary section
if ! echo "$README_CONTENT" | grep -q "$DIARY_SECTION_MARKER"; then
  echo "Error: Could not find '${DIARY_SECTION_MARKER}' section in ${README_PATH}"
  exit 1
fi

# Check if diary folder exists
if [ ! -d "$DIARY_FOLDER" ]; then
  echo "Error: Diary folder '$DIARY_FOLDER' does not exist"
  exit 1
fi

# Get list of all markdown files in the diary folder
DIARY_FILES=$(find "$DIARY_FOLDER" -type f -name "*.md" | sort)

# Exit if no diary files were found
if [ -z "$DIARY_FILES" ]; then
  echo "No diary files found in $DIARY_FOLDER"
  exit 0
fi

readme_modified=false

# Extract the Diary section header and everything before it
SECTION_START=$(grep -n "^${DIARY_SECTION_MARKER}" "$README_PATH" | cut -d':' -f1)
if [ -z "$SECTION_START" ]; then
  echo "Error: Could not find '${DIARY_SECTION_MARKER}' section in ${README_PATH}"
  exit 1
fi

# Create a temporary file with just the content before the Diary entries
head -n "$SECTION_START" "$README_PATH" > "$README_PATH.tmp"
echo "" >> "$README_PATH.tmp"  # Add a blank line after the section header

# Process each diary file and add to the temporary file
for DIARY_FILE in $DIARY_FILES; do
  # Extract the date from the filename (assuming format YYYY-MM-DD.md)
  FILENAME=$(basename "$DIARY_FILE")
  DATE=${FILENAME%.md}
  
  # Get the first heading from the diary file as the title
  if [ -f "$DIARY_FILE" ]; then
    TITLE=$(grep -m 1 "^# " "$DIARY_FILE" | sed 's/^# //' || echo "New entry")
  else
    TITLE="New entry"
  fi
  
  # Create the diary entry line and add it to the temp file
  DIARY_ENTRY="- ${DATE}: [${TITLE}](${DIARY_FILE})"
  echo "$DIARY_ENTRY" >> "$README_PATH.tmp"
  readme_modified=true
done

# Replace the original README with the updated version
if [ "$readme_modified" = true ]; then
  mv "$README_PATH.tmp" "$README_PATH"
  
  # If running as part of a git hook, re-stage README.md
  if git rev-parse --git-dir > /dev/null 2>&1; then
    git add "$README_PATH"
  fi
  
  echo "Updated README.md with all diary entries"
else
  rm "$README_PATH.tmp"
  echo "No changes made to README.md"
fi

exit 0
