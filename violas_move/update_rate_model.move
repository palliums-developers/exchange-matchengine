script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main<Token:store>(account: signer, base_rate: u64, rate_multiplier: u64, rate_jump_multiplier: u64, rate_kink: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::update_rate_model<Token>(&account, base_rate, rate_multiplier, rate_jump_multiplier, rate_kink);
}
}
