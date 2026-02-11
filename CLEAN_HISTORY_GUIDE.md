# Guide: Cleaning Git History to a Single Initial Commit

This guide explains how to remove all Git history from your repository and start with a clean, single initial commit containing the current state of all files.

## Why This Guide?

The automated system cannot perform force pushes, which are required to rewrite Git history. Therefore, you'll need to manually execute these commands on your local machine.

## Prerequisites

- Git installed on your local machine
- Write access to the repository
- A local clone of the repository

## Steps to Clean Git History

### 1. Clone the Repository (if not already done)

```bash
git clone https://github.com/jonathandhaene/azure-servicebus-largemessage.git
cd azure-servicebus-largemessage
```

### 2. Create a New Orphan Branch

An orphan branch has no parent commits, starting fresh:

```bash
git checkout --orphan clean-history
```

### 3. Stage All Files

Add all current files to the new branch:

```bash
git add -A
```

### 4. Create the Initial Commit

Create a single commit with all files:

```bash
git commit -m "Initial commit"
```

### 5. Verify Single Commit

Check that you only have one commit:

```bash
git log --oneline
# Should show only one commit
```

### 6. Replace the Main Branch

**⚠️ WARNING: This will permanently delete all previous history!**

Back up your repository before proceeding!

```bash
# Delete the old main branch locally
git branch -D main  # or master, depending on your default branch name

# Rename the clean branch to main
git branch -m main  # or master

# Force push to replace remote history
git push -f origin main  # or master
```

### 7. Update Other Branches (Optional)

If you have other branches you want to keep, you'll need to rebase them onto the new clean history or recreate them from the new main branch.

## Alternative: Keep Main and Create Clean Branch

If you want to preserve the old history on the main branch and just create a new clean branch:

```bash
# After step 4 above (creating the clean-history branch)
git push -f origin clean-history

# Then you can merge or replace main later
```

## Verification

After completing the steps, verify:

```bash
# Check commit count (should be 1)
git log --oneline | wc -l

# Check repository size (should be smaller)
du -sh .git

# Verify all files are present
git ls-files
```

## What This Changes

- **Repository history**: Replaced with a single commit
- **.git folder size**: Will be significantly smaller
- **Commit SHAs**: All commit IDs will change
- **Tags and releases**: Will still reference old commits (consider recreating)
- **Open PRs**: May need to be recreated
- **Collaborators**: Will need to re-clone the repository

## Collaborator Instructions

After you've cleaned the history, all collaborators should:

```bash
# Delete their local repository and re-clone
cd ..
rm -rf azure-servicebus-largemessage
git clone https://github.com/jonathandhaene/azure-servicebus-largemessage.git
```

Or if they want to keep local changes:

```bash
cd azure-servicebus-largemessage
git fetch origin
git reset --hard origin/main  # This will lose local changes!
```

## Troubleshooting

### "Push rejected" error

Make sure you're using the `-f` (force) flag:
```bash
git push -f origin main
```

### "Protected branch" error

You may need to temporarily disable branch protection rules in GitHub:
1. Go to repository Settings → Branches
2. Temporarily remove branch protection for the main branch
3. Push your changes
4. Re-enable branch protection

### Want to keep specific tags

Before cleaning history, export important tag information:
```bash
git show-ref --tags > tags-backup.txt
```

After cleaning, recreate tags on the new commit:
```bash
git tag v1.0.0
git push origin v1.0.0
```

## Notes

- This operation is **irreversible** once pushed to the remote
- Always create a backup before starting
- Consider archiving the old repository if the history is important
- Update any documentation that references specific commit SHAs

## Questions?

If you encounter issues not covered in this guide, consider:
- Checking Git documentation: https://git-scm.com/docs
- GitHub's guide on removing sensitive data (similar concepts)
- Consulting with your team before making destructive changes
