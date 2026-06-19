@echo off
setlocal EnableExtensions

:: Set directory to the location of this batch file (CRITICAL for Task Scheduler)
set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul || (
    echo [ERROR] Could not switch to script directory: "%SCRIPT_DIR%"
    exit /b 1
)

:: Find Python executable
set "PY_CMD="
where py >nul 2>&1 && set "PY_CMD=py -3"
if not defined PY_CMD (
    where python >nul 2>&1 && set "PY_CMD=python"
)

if not defined PY_CMD (
    echo [ERROR] Python was not found. Install Python or add it to PATH.
    popd >nul
    exit /b 9009
)

set "DRY_RUN=0"
if /I "%~1"=="--dry-run" set "DRY_RUN=1"

echo ================================================================
echo Starting pipeline: Playwright_test -^> NHTSA_enrichment -^> DataCleaning
echo Working directory: %CD%
echo Python command   : %PY_CMD%
echo ================================================================

:: --- STEP 1: PLAYWRIGHT SCRAPING ---
echo [1/3] Running Playwright_test...
if "%DRY_RUN%"=="1" (
    echo [DRY RUN] %PY_CMD% "DataPipeline\Playwright_test.py"
) else (
    %PY_CMD% "DataPipeline\Playwright_test.py"
    if errorlevel 1 (
        set "ERR=%errorlevel%"
        echo [ERROR] Playwright_test failed with exit code %ERR%.
        popd >nul
        exit /b %ERR%
    )
)

:: --- STEP 2: NHTSA ENRICHMENT ---
echo [2/3] Running NHTSA_enrichment...
if "%DRY_RUN%"=="1" (
    echo [DRY RUN] %PY_CMD% "DataPipeline\NHTSA_enrichment.py"
) else (
    %PY_CMD% "DataPipeline\NHTSA_enrichment.py"
    if errorlevel 1 (
        set "ERR=%errorlevel%"
        echo [ERROR] NHTSA_enrichment failed with exit code %ERR%.
        popd >nul
        exit /b %ERR%
    )
)

:: --- STEP 3: DATA CLEANING ---
echo [3/3] Running DataCleaning...
if "%DRY_RUN%"=="1" (
    echo [DRY RUN] %PY_CMD% "DataPipeline\DataCleaning.py"
) else (
    %PY_CMD% "DataPipeline\DataCleaning.py"
    if errorlevel 1 (
        set "ERR=%errorlevel%"
        echo [ERROR] DataCleaning failed with exit code %ERR%.
        popd >nul
        exit /b %ERR%
    )
)

echo ================================================================
echo [SUCCESS] Pipeline completed successfully!
echo ================================================================
popd >nul
exit /b 0