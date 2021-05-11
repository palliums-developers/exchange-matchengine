script {
use 0x1::ViolasBank;

fun main<Token:store>(account: signer, factor: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::update_collateral_factor<Token>(&account, factor);
}
}
