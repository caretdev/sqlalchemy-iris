#!/bin/bash

iris_start () {
  iris start iris

  # Reset Password changing and activate %Service_CallIn for Embedded Python
  cat <<EOF | iris session iris -U %SYS
do ##class(Security.Users).UnExpireUserPasswords("*")
set prop("Enabled")=1 
Do ##class(Security.Services).Modify("%Service_CallIn",.prop) 
halt
EOF
}

iris_stop () {
  echo "Stopping IRIS"
  iris stop iris quietly
}

exit_on_error () {
  exit=$?;
  if [ $exit -ne 0 ]; then
    iris_stop
    exit $exit
  fi
}

iris_start

/usr/irissys/bin/irispython -m pip install -r requirements-dev.txt -r requirements-iris.txt --target /usr/irissys/mgr/python
exit_on_error

# Test in Network mode
/usr/irissys/bin/irispython -m pytest
exit_on_error

# Test in Embedded mode
/usr/irissys/bin/irispython -m pytest --db irisemb
exit_on_error

iris_stop
