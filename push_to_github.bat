@echo off
title Push AdventureWorks Lakehouse to GitHub
echo ========================================================
echo   Pushing AdventureWorks Medallion Lakehouse to GitHub
echo ========================================================
echo.
echo Pushing to https://github.com/Kartikeya-Rana-2004/Adventure-works-dataset-project.git ...
echo.
git add -A
git commit -m "feat: add PySpark medallion ETL, ADF pipeline, dbt tests, executive live dashboard, and architecture docs"
git push -u origin main
echo.
if %ERRORLEVEL% EQU 0 (
    echo ========================================================
    echo [SUCCESS] Pushed to GitHub successfully!
    echo.
    echo Next step: Enable GitHub Pages in your repo settings:
    echo 1. Go to: https://github.com/Kartikeya-Rana-2004/Adventure-works-dataset-project/settings/pages
    echo 2. Under 'Build and deployment', choose 'GitHub Actions'
    echo    (The included workflow will deploy automatically!)
    echo.
    echo Your live dashboard will be online at:
    echo https://kartikeya-rana-2004.github.io/Adventure-works-dataset-project/
    echo ========================================================
) else (
    echo [ERROR] Push failed.
)
echo.
pause
