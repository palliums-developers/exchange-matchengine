
g++ main.cpp remotedb.cpp financial_management.cpp ../utils/utilities.cpp \
    -std=c++1z -Wfatal-errors -g -o a.out \
    -lmariadbclient -pthread -ldl -lssl -lcrypto \
    



