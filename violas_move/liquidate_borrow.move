script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main<Token1, Token2>(account: &signer, borrower: address, amount: u64, data: vector<u8>) {
    if(ViolasBank::is_published(account) == false) {
	ViolasBank::publish(account, x"00");
    };
    ViolasBank::liquidate_borrow<Token1, Token2>(account, borrower, amount, data);
}
}

