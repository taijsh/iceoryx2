#include "iox2/iceoryx2.h"
int main() {
    iox2_waitset_h waitset = NULL;
    iox2_waitset_attach_interval(&waitset, 100);
    return 0;
}
