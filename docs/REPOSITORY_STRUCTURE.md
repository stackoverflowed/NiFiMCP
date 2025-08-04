# Repository Structure and Fork Maintenance

This repository is now maintained as a fork of the original NiFiMCP project with additional Claude Desktop support.

## Repository Structure

### Remote Configuration

The repository has three remotes configured:

1. **`forestrat`** - Main repository in Forestrat-Inc organization
   - URL: `https://github.com/Forestrat-Inc/NiFiMCP.git`
   - Purpose: Primary repository for Forestrat-Inc organization
   - Push target: `git push forestrat master`

2. **`origin`** - Personal fork (stackoverflowed)
   - URL: `https://github.com/stackoverflowed/NiFiMCP.git`
   - Purpose: Personal development fork
   - Push target: `git push origin master`

3. **`upstream`** - Original repository
   - URL: `https://github.com/ms82119/NiFiMCP.git`
   - Purpose: Source of upstream changes
   - Fetch target: `git fetch upstream`

### Branch Strategy

- **`master`** - Main development branch with Claude Desktop enhancements
- **`upstream/master`** - Original repository's main branch

## Fork Maintenance

### Syncing with Upstream

To keep the fork updated with the original repository:

```bash
# Fetch latest changes from upstream
git fetch upstream

# Merge upstream changes into local master
git checkout master
git merge upstream/master

# Push to both remotes
git push forestrat master
git push origin master
```

### Development Workflow

1. **Create feature branches from master:**
   ```bash
   git checkout -b feature/new-feature
   ```

2. **Make changes and commit:**
   ```bash
   git add .
   git commit -m "Add new feature"
   ```

3. **Push to personal fork for testing:**
   ```bash
   git push origin feature/new-feature
   ```

4. **Merge to master and push to organization:**
   ```bash
   git checkout master
   git merge feature/new-feature
   git push forestrat master
   ```

### Pull Request Strategy

- **To upstream:** Create PRs from `forestrat/master` to `upstream/master`
- **Internal:** Create PRs from feature branches to `forestrat/master`

## Repository URLs

- **Organization Repository:** https://github.com/Forestrat-Inc/NiFiMCP
- **Personal Fork:** https://github.com/stackoverflowed/NiFiMCP
- **Original Repository:** https://github.com/ms82119/NiFiMCP

## Key Differences from Original

This fork includes several enhancements over the original:

1. **Claude Desktop Support** - Full MCP server integration
2. **stdio Transport** - Native Claude Desktop compatibility
3. **Enhanced Documentation** - Comprehensive setup guides
4. **Improved Error Handling** - Better JSON protocol compliance
5. **Multiple Transport Options** - Both stdio and SSE support

## Maintenance Commands

### Check Remote Status
```bash
git remote -v
```

### Fetch All Remotes
```bash
git fetch --all
```

### View Branch Status
```bash
git branch -vv
```

### Sync with Upstream
```bash
git fetch upstream
git merge upstream/master
git push forestrat master
```

## Contributing

1. Fork the repository to your personal account
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Create a pull request to the Forestrat-Inc organization
6. After review and approval, merge to master

## Release Strategy

1. **Development:** Use feature branches
2. **Testing:** Push to personal fork first
3. **Release:** Merge to `forestrat/master`
4. **Upstream:** Create PRs to original repository as needed

## Backup Strategy

- **Primary:** Forestrat-Inc organization repository
- **Secondary:** Personal fork (stackoverflowed)
- **Source:** Original repository (ms82119)

This ensures redundancy and maintains the fork relationship with the original project. 