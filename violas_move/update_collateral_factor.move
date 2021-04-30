script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main<Token:store>(account: signer, factor: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::update_collateral_factor<Token>(&account, factor);
}
}
