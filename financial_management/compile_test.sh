
g++ test.cpp ../utils/utilities.cpp \
    -std=c++1z -Wfatal-errors -g -o test \
    -lmariadbclient -pthread -ldl -lssl -lcrypto \
    



