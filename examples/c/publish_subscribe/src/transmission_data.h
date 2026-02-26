// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

#ifndef IOX2_EXAMPLES_TRANSMISSION_DATA_H
#define IOX2_EXAMPLES_TRANSMISSION_DATA_H

#include <stdint.h>

struct TransmissionCommonData {
    char header[512];
    char payload[64 * 1024]; // 64KB
};

struct TransmissionLargeData {
    char header[512];
    char payload[2 * 1024 * 1024]; // 2MB
};

struct TransmissionStreamData {
    char header[512];
    char payload[32 * 1024 * 1024]; // 32MB
};

#endif
