
g++ main.cpp remotedb.cpp financial_management.cpp ../utils/utilities.cpp \
    -L /usr/local/lib/mariadb -std=c++1z -Wfatal-errors -g -o a.out \
    -lmariadbclient -pthread -ldl -lssl -lcrypto \
    



