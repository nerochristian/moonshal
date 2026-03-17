@echo off
setlocal EnableExtensions

set "REPO_URL=https://github.com/nerochristian/moonshal.git"
set "BRANCH=main"
set "APP_DIR=%~dp0"

cd /d "%APP_DIR%"

where git >nul 2>nul
if errorlevel 1 (
    echo Git is not installed or not in PATH.
    goto :end
)

echo [1/5] Preparing git repository...
if not exist ".git" (
    git init
    if errorlevel 1 goto :end
)

git branch --show-current >nul 2>nul
if errorlevel 1 (
    git checkout -b %BRANCH%
) else (
    git checkout %BRANCH% 2>nul
    if errorlevel 1 git checkout -b %BRANCH%
)
if errorlevel 1 goto :end

echo [2/5] Configuring remote...
git remote remove origin >nul 2>nul
git remote add origin "%REPO_URL%"
if errorlevel 1 goto :end

echo [3/5] Staging files...
git add .
if errorlevel 1 goto :end

git diff --cached --quiet
if not errorlevel 1 (
    echo Nothing to push.
    goto :end
)

echo [4/5] Creating commit...
git commit -m "Update bot files"
if errorlevel 1 (
    echo Commit failed. Check git user config or repo state.
    goto :end
)

echo [5/5] Pushing to GitHub...
git push -u origin %BRANCH%
if errorlevel 1 (
    echo Push failed. You may need to authenticate with GitHub.
    goto :end
)

echo Push complete.

:end
echo.
pause
exit /b
