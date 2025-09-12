# Git Repo Workdir Integration

This example demonstrates how to sync contents from a Git repo to a remote cluster so that you can run your code on the cluster.

## Prerequisites (Optional, for private repos only):
- Generate an [SSH Key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) or [access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens), or use an existing one
- Set `SKYPILOT_GIT_SSH_KEY_PATH` or `SKYPILOT_GIT_TOKEN` in your Airflow variables. Refer to [Using Private Git Repo for Workdir](../../README.md#optional-using-private-git-repo-for-workdir) for more details
