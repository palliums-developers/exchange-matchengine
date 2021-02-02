addr=$1

cp stdlib/pbank.move.0 stdlib/modules/pbank.move

cp t1/*.move ./
sed -i "s/7257c2417e4d1038e1817c8f283ace2e/$addr/g" *.move
sed -i "s/ViolasBank2/ViolasBank/g" b1.sh
./b1.sh
cp *.mv t1/

cp bank.move stdlib/modules/pbank.move

cp t2/*.move ./
sed -i "s/7257c2417e4d1038e1817c8f283ace2e/$addr/g" *.move
sed -i "s/ViolasBank/ViolasBank2/g" b1.sh
./b1.sh
cp *.mv t2/


