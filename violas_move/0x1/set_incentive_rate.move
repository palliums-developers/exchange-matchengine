script {
use 0x1::ViolasBank;

fun main(account: signer, rate: u64) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::set_incentive_rate(&account, rate);
}
}
