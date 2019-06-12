test_arch="0"
test_arch="$test_arch 1 0"
test_arch="$test_arch 2 1 0"
test_arch="$test_arch 3 1 0 1"
test_arch="$test_arch 4 1 1 0 0"
test_arch="$test_arch 5 1 1 0 0 1"

# Final neural architecture found by ENAS
# Source: https://github.com/melodyguan/enas/blob/master/scripts/cifar10_macro_final.sh
fixed_arc="0"
fixed_arc="$fixed_arc 3 0"
fixed_arc="$fixed_arc 0 1 0"
fixed_arc="$fixed_arc 2 0 0 1"
fixed_arc="$fixed_arc 2 0 0 0 0"
fixed_arc="$fixed_arc 3 1 1 0 1 0"
fixed_arc="$fixed_arc 2 0 0 0 0 0 1"
fixed_arc="$fixed_arc 2 0 1 1 0 1 1 1"
fixed_arc="$fixed_arc 1 0 1 1 1 0 1 0 1"
fixed_arc="$fixed_arc 0 0 0 0 0 0 0 0 0 0"
fixed_arc="$fixed_arc 2 0 0 0 0 0 1 0 0 0 0"
fixed_arc="$fixed_arc 0 1 0 0 1 1 0 0 0 0 1 1"
fixed_arc="$fixed_arc 2 0 1 0 0 0 0 0 1 0 1 1 0"
fixed_arc="$fixed_arc 1 0 0 1 0 0 0 1 1 1 0 1 0 1"
fixed_arc="$fixed_arc 0 1 1 0 1 0 1 0 0 0 0 0 1 0 0"
fixed_arc="$fixed_arc 2 0 0 1 0 0 0 0 0 0 0 1 0 1 0 1"
fixed_arc="$fixed_arc 2 0 1 0 0 0 1 0 0 1 1 1 1 0 0 1 0"
fixed_arc="$fixed_arc 2 0 0 0 0 1 0 1 0 1 0 0 1 0 1 0 0 1"
fixed_arc="$fixed_arc 3 0 1 1 0 1 0 0 0 0 0 1 0 1 0 1 0 0 0"
fixed_arc="$fixed_arc 3 0 1 1 0 0 0 0 0 0 0 0 0 1 1 0 0 0 0 1"
fixed_arc="$fixed_arc 0 1 0 0 1 0 1 1 0 0 0 1 0 0 0 0 0 1 1 0 0"
fixed_arc="$fixed_arc 3 0 1 0 1 1 0 0 1 0 1 1 0 1 1 0 1 0 0 1 0 0"
fixed_arc="$fixed_arc 0 1 0 1 0 1 0 0 0 0 0 0 0 0 1 0 1 0 0 1 0 0 0"
fixed_arc="$fixed_arc 0 1 1 0 0 0 1 1 1 0 1 0 0 0 1 0 1 0 0 1 1 0 0 0"
enas_final="$fixed_arc"

python -m autofl.agent \
  --arch="${enas_final}" \
  "$@"