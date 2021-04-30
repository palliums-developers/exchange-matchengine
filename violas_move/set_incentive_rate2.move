script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(account: signer, rate: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::set_incentive_rate2(&account, rate);
}
}
