
g++ client.cpp ../utils/utilities.cpp \
    -L /usr/local/lib/mariadb -std=c++1z -Wfatal-errors -g -o client \
    -lmariadbclient -pthread -ldl -lssl -lcrypto \
    
