script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main<Token:store>(account: signer, amount: u64, data: vector<u8>) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    if(amount == 0) {
	amount = ViolasBank::borrow_balance<Token>(&account);
    };
    ViolasBank::enter_bank<Token>(&account, amount);
    ViolasBank::repay_borrow<Token>(&account, amount, data);
}
}
