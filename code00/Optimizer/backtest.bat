@echo off
::title Optimizer_python

:: ��ȡ��ǰ�ű����ڵ�Ŀ¼
set "SCRIPT_DIR=%~dp0"
set "PYTHONPATH=%SCRIPT_DIR%"
set "ANACONDAPATH=%ANACONDA_PATH%"

for /f "tokens=1-3 delims=:." %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

setlocal enabledelayedexpansion
echo Start Time:%time%
set PATH_2=%SCRIPT_DIR%Optimizer_python\history_manual_main

:: ���е�һ���ָ���
echo Running main_updating_part1...
cd /d "%SCRIPT_DIR%\Optimizer_python\history_manual_main"
%ANACONDAPATH%\python -c "from manual_main import backtest_main_part1; backtest_main_part1()"

:: ��ȡPython�ű����˳�״̬
set ERRORLEVEL=%ERRORLEVEL%
echo %ERRORLEVEL%

:: ѭ������ÿ������ļ�
for /l %%i in (1,1,%ERRORLEVEL%) do (
	"%MATLAB_PATH%\matlab.exe" -nodisplay -nosplash -nodesktop -r "addpath('%SCRIPT_DIR%\Optimizer_python\matlab');optimizer_matlab_V7(%%i,'%PATH_2%\\output_part1%%i.txt'); exit;"
)

:: ��ʼ��������
set /a count=0

:: ѭ���ȴ�MATLAB�ű����
:loop
set /a count+=30
timeout /t 30 >nul
set /a num=0
for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist result%%i.txt set /a num+=1
)

:: ����Ƿ����н���ļ���������
if !num! neq %ERRORLEVEL% goto loop

:: �������60���ӣ�����ѭ��
if !count! gtr 3600 goto end

echo MATLAB scripts have finished.

:: ���еڶ����ָ���
echo Running main_updating_part2...
cd /d %SCRIPT_DIR%\Optimizer_Backtesting
%ANACONDAPATH%\python -c "from optimizer_backtesting_main import history_running_main; history_running_main()"

cd /d "%SCRIPT_DIR%\Optimizer_python\history_manual_main"
:: ������ʱ�ļ�
for /l %%i in (1,1,%ERRORLEVEL%) do (
    if exist output_part1%%i.txt del /F output_part1%%i.txt
    if exist result%%i.txt del /F result%%i.txt
)


:end
echo End Time:%time%
echo Script execution completed.

pause