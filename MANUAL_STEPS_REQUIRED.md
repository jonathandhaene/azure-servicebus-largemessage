# Git History Cleaning - Manual Steps Required

## Summary

Due to system constraints (force push is not available in the automated environment), the Git history cleaning cannot be completed automatically. This PR provides you with comprehensive guides and tools to complete the process manually.

## What's Included

1. **CLEAN_HISTORY_GUIDE.md** - Detailed step-by-step guide explaining how to clean Git history
2. **clean-history.sh** - Automated script to execute the cleaning process
3. **.history-cleaned** - Marker file documenting this change

## Quick Start

### Option 1: Use the Automated Script (Recommended)

```bash
# Clone or navigate to your repository
cd azure-servicebus-largemessage

# Checkout this PR branch
git checkout copilot/remove-git-history

# Run the cleaning script
./clean-history.sh
```

The script will:
- Guide you through the process with prompts
- Create a new branch with clean history
- Optionally push the changes
- Provide safety confirmations at each destructive step

### Option 2: Manual Process

Follow the step-by-step instructions in `CLEAN_HISTORY_GUIDE.md`.

## What This Does

- Removes ALL Git commit history
- Creates a single "Initial commit" with all current files
- Significantly reduces `.git` folder size
- Makes the repository appear as a fresh, clean push

## Important Warnings

⚠️ **This operation is IRREVERSIBLE once pushed!**

- All commit history will be permanently deleted
- All commit SHAs will change
- All collaborators will need to re-clone the repository
- Open PRs may need to be recreated
- Branch protection rules may need to be temporarily disabled

## Before You Start

1. **Create a backup** of your repository
2. **Notify your team** about the planned change
3. **Close or merge any important PRs** before cleaning history
4. **Document any important commits** you want to reference later

## After Cleaning History

1. Inform all collaborators to re-clone the repository
2. Re-create any important tags or releases
3. Re-enable any branch protection rules you disabled
4. Update any documentation that references specific commit SHAs

## Why Manual?

The automated system operates within a sandboxed environment that doesn't allow force pushing (for safety reasons). Force pushing is required to rewrite Git history on the remote repository. Therefore, this operation must be performed from your local machine where you have full control.

## Questions or Issues?

Refer to the comprehensive troubleshooting section in `CLEAN_HISTORY_GUIDE.md` or consult Git documentation.
