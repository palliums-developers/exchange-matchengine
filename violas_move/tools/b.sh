addr=$1

rm -f *.move
rm -f *.mv
rm -f *.mv.1
cp /home/ops/lmf/github.com/exchange-matchengine/violas_move/*.move ./

sed -i "s/7257c2417e4d1038e1817c8f283ace2e1041b3396cdbb099eb357bbee024d614/$addr/g" *.move

../move-build -f token.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move 


mv output/transaction_0_module_ViolasToken.mv token.mv

../move-build -f publish.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv publish.mv

../move-build -f mint.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv mint.mv

../move-build -f transfer.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv transfer.mv

../move-build -f create_token.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv create_token.mv

../move-build -f make_order.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv make_order.mv

../move-build -f cancel_order.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv cancel_order.mv

../move-build -f take_order.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv take_order.mv

../move-build -f move_owner.move -s 0x$addr \
	      -d \
	      modules/libra_account.move \
	      modules/libra_coin.move \
	      modules/vector.move \
	      modules/transaction.move \
	      modules/u64_util.move \
	      modules/libra_time.move \
	      modules/address_util.move \
	      modules/hash.move \
	      modules/libra_transaction_timeout.move \
	      token.move

mv output/transaction_0_script.mv move_owner.mv

./bin2json token.mv
./bin2json publish.mv
./bin2json mint.mv
./bin2json transfer.mv
./bin2json create_token.mv
./bin2json make_order.mv
./bin2json cancel_order.mv
./bin2json take_order.mv
./bin2json move_owner.mv

mv token.mv.1 token.mv
mv publish.mv.1 publish.mv
mv mint.mv.1 mint.mv
mv transfer.mv.1 transfer.mv
mv create_token.mv.1 create_token.mv
mv make_order.mv.1 make_order.mv
mv cancel_order.mv.1 cancel_order.mv
mv take_order.mv.1 take_order.mv
mv move_owner.mv.1 move_owner.mv
