script {
use 0x1::ViolasBank;

fun main<Token:store>(account: signer, amount: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::enter_bank<Token>(&account, amount);
}
}
