
g++ client.cpp ../utils/utilities.cpp \
    -std=c++1z -Wfatal-errors -g -o client \
    -lmariadbclient -pthread -ldl -lssl -lcrypto \
    
