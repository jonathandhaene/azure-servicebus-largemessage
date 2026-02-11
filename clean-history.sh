#!/bin/bash

# Script to clean Git history to a single initial commit
# USE WITH CAUTION: This will permanently delete all Git history!

set -e  # Exit on error

echo "======================================"
echo "Git History Cleaning Script"
echo "======================================"
echo ""
echo "⚠️  WARNING: This will PERMANENTLY DELETE all Git history!"
echo "⚠️  Make sure you have a backup before proceeding!"
echo ""
read -p "Are you sure you want to continue? (type 'yes' to proceed): " confirmation

if [ "$confirmation" != "yes" ]; then
    echo "Operation cancelled."
    exit 0
fi

echo ""
echo "Step 1: Checking current branch..."
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo "Current branch: $CURRENT_BRANCH"

echo ""
echo "Step 2: Creating orphan branch..."
git checkout --orphan temp-clean-history

echo ""
echo "Step 3: Staging all files..."
git add -A

echo ""
echo "Step 4: Creating initial commit..."
git commit -m "Initial commit"

echo ""
echo "Step 5: Verifying single commit..."
COMMIT_COUNT=$(git log --oneline | wc -l)
echo "Commit count: $COMMIT_COUNT"

if [ "$COMMIT_COUNT" != "1" ]; then
    echo "❌ Error: Expected 1 commit, found $COMMIT_COUNT"
    exit 1
fi

echo "✅ Single commit verified"

echo ""
echo "Step 6: Replacing branch..."
read -p "Enter the branch name to replace (e.g., 'main' or 'master'): " TARGET_BRANCH

if [ -z "$TARGET_BRANCH" ]; then
    echo "❌ Error: Branch name cannot be empty"
    exit 1
fi

# Delete old branch locally
git branch -D "$TARGET_BRANCH" 2>/dev/null || echo "Branch $TARGET_BRANCH doesn't exist locally, continuing..."

# Rename clean branch to target
git branch -m "$TARGET_BRANCH"

echo ""
echo "Step 7: Preparing to force push..."
echo "⚠️  This will PERMANENTLY DELETE remote history!"
read -p "Force push to origin/$TARGET_BRANCH? (type 'yes' to proceed): " push_confirmation

if [ "$push_confirmation" != "yes" ]; then
    echo "Push cancelled. Your local branch has been cleaned."
    echo "To push later, run: git push -f origin $TARGET_BRANCH"
    exit 0
fi

echo ""
echo "Force pushing to origin/$TARGET_BRANCH..."
git push -f origin "$TARGET_BRANCH"

echo ""
echo "✅ Git history successfully cleaned!"
echo ""
echo "Repository now has a single initial commit."
echo ""
echo "⚠️  IMPORTANT: All collaborators should re-clone the repository or run:"
echo "    git fetch origin"
echo "    git reset --hard origin/$TARGET_BRANCH"
echo ""
echo "Commit details:"
git log --oneline
