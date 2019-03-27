
g++ main.cpp engine.cpp localdb.cpp remotedb.cpp ../utils/utilities.cpp \
    -std=c++1z -Wfatal-errors -g -o match_engine \
    -lrocksdb -lsnappy -lmariadbclient -pthread -ldl -lssl -lcrypto \
    



