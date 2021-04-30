script {
use 0x7257c2417e4d1038e1817c8f283ace2e::ViolasBank;

fun main(account: signer) {
    if(ViolasBank::is_published(&account) == false) {
	ViolasBank::publish(&account, x"00");
    };
    ViolasBank::claim_incentive(&account);
}
}
