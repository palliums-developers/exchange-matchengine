script {
use 0x1::ViolasBank;

fun main<Token:store>(account: signer, price: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::update_price<Token>(&account, price);
}
}
