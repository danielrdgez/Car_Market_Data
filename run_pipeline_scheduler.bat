@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul || (
    echo [ERROR] Could not switch to script directory: "%SCRIPT_DIR%"
    exit /b 1
)

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
echo Starting pipeline: DataAquisition -^> NHTSA_enrichment
echo Working directory: %CD%
echo Python command   : %PY_CMD%
echo ================================================================

echo [1/2] Running DataAquisition...
if "%DRY_RUN%"=="1" (
    echo [DRY RUN] %PY_CMD% "DataPipeline\DataAquisition.py"
) else (
    %PY_CMD% "DataPipeline\DataAquisition.py"
    if errorlevel 1 (
        set "ERR=%errorlevel%"
        echo [ERROR] DataAquisition failed with exit code %ERR%.
        popd >nul
        exit /b %ERR%
    )
)

echo [2/2] Running NHTSA_enrichment...
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

echo ================================================================
echo Pipeline finished successfully.
echo ================================================================

popd >nul
exit /b 0

