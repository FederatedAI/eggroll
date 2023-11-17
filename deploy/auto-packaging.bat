@echo off

setlocal

set "pwd=%cd%"
set "cwd=%~dp0"
cd /d "%cwd%"

for /f "usebackq tokens=2 delims==" %%G in (`findstr /i "version" ..\BUILD_INFO`) do set "version=%%G"

cd ..\jvm
mvn clean package -DskipTests

cd ..

if not exist "lib" (
    mkdir lib
)

del /s /q lib\*.jar

xcopy /y /e jvm\core\target\core-%version%.jar lib\
xcopy /y /e jvm\core\target\lib\* lib\
xcopy /y /e jvm\cluster_manager\target\cluster_manager-%version%.jar lib\
xcopy /y /e jvm\cluster_manager\target\lib\* lib\
xcopy /y /e jvm\node_manager\target\node_manager-%version%.jar lib\
xcopy /y /e jvm\node_manager\target\lib\* lib\
xcopy /y /e jvm\cluster_dashboard\target\cluster_dashboard-%version%.jar lib\
xcopy /y /e jvm\cluster_dashboard\target\lib\* lib\

tar -czf eggroll.tar.gz lib bin conf data python deploy

cd /d "%pwd%"

endlocal