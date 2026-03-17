@echo off
setlocal EnableExtensions

set "REPO_URL=https://github.com/nerochristian/moonshal.git"
set "BRANCH=main"
set "APP_DIR=%~dp0"

cd /d "%APP_DIR%"

where git >nul 2>nul
if errorlevel 1 (
    echo Git is not installed or not in PATH.
    exit /b 1
)

echo [1/5] Preparing git repository...
if not exist ".git" (
    git init
    if errorlevel 1 exit /b 1
)

git branch --show-current >nul 2>nul
if errorlevel 1 (
    git checkout -b %BRANCH%
) else (
    git checkout %BRANCH% 2>nul
    if errorlevel 1 git checkout -b %BRANCH%
)
if errorlevel 1 exit /b 1

echo [2/5] Configuring remote...
git remote remove origin >nul 2>nul
git remote add origin "%REPO_URL%"
if errorlevel 1 exit /b 1

echo [3/5] Staging files...
git add .
if errorlevel 1 exit /b 1

git diff --cached --quiet
if not errorlevel 1 (
    echo Nothing to push.
    exit /b 0
)

echo [4/5] Creating commit...
git commit -m "Update bot files"
if errorlevel 1 (
    echo Commit failed. Check git user config or repo state.
    exit /b 1
)

echo [5/5] Pushing to GitHub...
git push -u origin %BRANCH%
if errorlevel 1 (
    echo Push failed. You may need to authenticate with GitHub.
    exit /b 1
)

echo Push complete.
exit /b 0
