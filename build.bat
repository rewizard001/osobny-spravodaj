@echo off
setlocal

REM Create venv if it doesn't exist
if not exist .venv (
  python -m venv .venv
)

call .venv\Scripts\activate

python -m pip install --upgrade pip
pip install -r requirements.txt

python tools\osobny_spravodaj_build_config.py --input config\source_registry.xlsx --outdir build --format both --schema registry.schema.json
if errorlevel 1 exit /b 1

echo.
echo BUILD OK
endlocal
