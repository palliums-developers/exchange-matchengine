script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main<Token:store>(account: signer, amount: u64, data: vector<u8>) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::enter_bank<Token>(&account, amount);
    ViolasBank::lock<Token>(&account, amount, data);
}
}
