script {
use 0x1::ViolasBank;

fun main<Token1:store, Token2:store>(account: signer, borrower: address, amount: u64, data: vector<u8>) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::liquidate_borrow<Token1, Token2>(&account, borrower, amount, data);
}
}

