/*************************************************************************
 * The contents of this file are subject to the MYRICOM SNIFFER10G
 * LICENSE (the "License"); User may not use this file except in
 * compliance with the License.  The full text of the License can found
 * in LICENSE.TXT
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and
 * limitations under the License.
 *
 * Copyright 2008 by Myricom, Inc.  All rights reserved.
 ***********************************************************************/
/*
 * Simple program to demonstrate how to receive packets using
 * the Myricom Sniffer API.
 */

#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <signal.h>
#include <assert.h>
#include <sys/time.h>

#include <wireshark/wiretap/wtap.h>

#include "snf.h"

static void
dump_data(unsigned char *pg, int len)
{
  int i = 0;
  while (i < len) {
    printf
      ("%04d: %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x\n",
       i, pg[i], pg[i + 1], pg[i + 2], pg[i + 3], pg[i + 4], pg[i + 5],
       pg[i + 6], pg[i + 7], pg[i + 8], pg[i + 9], pg[i + 10], pg[i + 11],
       pg[i + 12], pg[i + 13], pg[i + 14], pg[i + 15]);
    i += 16;
  }
}

uint64_t num_pkts = 0;
unsigned int max_received_tv_delta = 0;
uint64_t num_bytes = 0;
snf_ring_t hring;
int finished = 0;
int interval = 1;

#define TOGGLE(i) ((i+ 1) & 1)
#define TV_TO_US(tv) ((tv)->tv_sec * 1000000 + (tv)->tv_usec)
unsigned int itvl_idx = 0;
struct itvl_stats {
  struct timeval tv;
  uint64_t usecs;
  uint64_t num_pkts;
  uint64_t num_bytes;
} itvl[2];

float secs_delta;

void
stats()
{
  struct snf_ring_stats stats;
  int rc;
  if ((rc = snf_ring_getstats(hring, &stats))) {
    perror("nic stats failed");
  }

  printf("\n");
  if (num_pkts == stats.ring_pkt_recv) {
    printf("Packets received:         %" PRIu64 "\n", num_pkts);
  } else {
    printf("Packets received,    app: %" PRIu64 ", ring: %" PRIu64 "\n",
           num_pkts, stats.ring_pkt_recv);
  }

  printf("Total bytes:              %" PRIu64 " (%" PRIu64 " MB)\n",
          num_bytes, num_bytes / 1024 / 1024);
  if (num_pkts > 0) {
    printf("Average Packet Length:    %" PRIu64 " bytes\n",
          num_bytes / num_pkts);
  }

  printf("Dropped, NIC overflow:    %" PRIu64 "\n", stats.nic_pkt_overflow);
  printf("Dropped, ring overflow:   %" PRIu64 "\n", stats.ring_pkt_overflow);
  printf("Dropped, bad:             %" PRIu64 "\n", stats.nic_pkt_bad);
  printf("Dropped, total:           %" PRIu64 "\n\n", stats.nic_pkt_overflow + stats.ring_pkt_overflow + stats.nic_pkt_bad);
}

void
print_periodic_stats(void)
{
  struct itvl_stats *this_itvl = &itvl[itvl_idx];
  struct itvl_stats *last_itvl = &itvl[TOGGLE(itvl_idx)];
  float delta_secs;
  uint64_t delta_pkts;
  uint64_t delta_bytes;
  uint32_t pps;
  float gbps;
  float bps;

  gettimeofday(&this_itvl->tv, NULL);
  this_itvl->usecs = TV_TO_US(&this_itvl->tv);
  this_itvl->num_pkts = num_pkts;
  this_itvl->num_bytes = num_bytes;
  delta_secs = (this_itvl->usecs - last_itvl->usecs) / 1000000.0;
  delta_pkts = this_itvl->num_pkts - last_itvl->num_pkts;
  delta_bytes = this_itvl->num_bytes - last_itvl->num_bytes;

  if (delta_pkts != 0) {
    pps = delta_pkts / delta_secs;
    bps = ((float) delta_bytes * 8) / delta_secs;
    gbps = bps / 1000000000.0;

    printf
      ("%" PRIu64 " pkts (%" PRIu64 "B) in %.3f secs (%u pps), Avg Pkt: %"
       PRIu64 ", BW (Gbps): %6.3f / %f bps\n", delta_pkts, delta_bytes, delta_secs,
       pps, delta_bytes / delta_pkts, gbps, bps);
    fflush(stdout);
  }

  itvl_idx = TOGGLE(itvl_idx);
}

void
sigexit(int sig)
{
  /*stats();*/
  /*exit(0);*/
  finished = 1;
}

void
sigalrm(int sig)
{
  print_periodic_stats();
  alarm(interval);
}

void
usage(void)
{
  printf("Usage: snf_capture [options]\n\n");
  printf("  -v: verbose\n");
  printf("  -t<interval>: print periodic statistics (each interval s, default is 1)\n");
  printf("  -b <board number>: Myri10G board number to use.\n");
  printf("  -p: poll for packets instead of blocking\n");
  printf("  -n <num pkts>: number of packet to receive (default: 0 - infinite)\n");
  printf("  -d <ring sz>: size of the receive ring in bytes or megabytes if < 1048576\n");
  printf("  -S <snap len>: display first <snap len> bytes of each packet\n");
  printf("  -w <filename>: write captured packets to file\n");
  printf("  -s <snap len>: snap packets to length when writing to file\n");
  printf("  -f<format>: write file in format (-f alone to see available formats)\n");
  exit(1);
}

static void
show_dump_file_error(const char *fname, int err)
{
  switch (err) {
  case ENOSPC:
    fprintf(stderr,
            "Not all the packets could be written to %s, no space left on the file system.\n",
            fname);
    break;
  case WTAP_ERR_CANT_CLOSE:
    fprintf(stderr,
            "%s couldn't be closed for some unknown reason.",
            fname);
    break;
  case WTAP_ERR_SHORT_WRITE:
    fprintf(stderr,
            "Not all the packets could be written to %s.",
            fname);
    break;
  default:
    fprintf(stderr,
            "An error occurred while writing to %s: %s.",
            fname, wtap_strerror(err));
    break;
  }
}

int
main(int argc, char **argv)
{
  int rc;
  snf_handle_t hsnf;
  struct snf_recv_req recv_req;
  char c;
  int periodic_stats = 0;
  int verbose = 0;
  int snap_print = 0;
  int inspect_pkt;
  int boardnum = 0;
  uint64_t pkts_expected = 0xffffffffffffffffULL;
  int open_flags = 0;
  uint64_t dataring_sz = 0;
  int timeout_ms = -1;
  char *dump_file = NULL;
  int dump_file_format = WTAP_FILE_PCAP;
  int dump_snap = 65535;
  wtap_dumper *file_dumper = NULL;
  struct wtap_pkthdr work_wtap_pkthdr;
  union wtap_pseudo_header work_wtap_pseudo_header;
  work_wtap_pseudo_header.eth.fcs_len = 0;

  /* get args */
  while ((c = getopt(argc, argv, "vt::b:pn:d:S:w:s:f::")) != -1) {
    if (c == 'v') {
      verbose++;
    } else if (c == 't') {
      periodic_stats = 1;
      if (optarg != 0) {
        interval = strtoul(optarg, NULL, 0);
      }
    } else if (c == 'b') {
      boardnum = strtoul(optarg, NULL, 0);
    } else if (c == 'p') {
      timeout_ms = 0;
    } else if (c == 'n') {
      pkts_expected = strtoul(optarg, NULL, 0);
    } else if (c == 'd') {
      dataring_sz = strtoull(optarg, NULL, 0);
    } else if (c == 'S') {
      snap_print = strtoul(optarg, NULL, 0); 
    } else if (c == 'w') {
      dump_file = strdup(optarg);
    } else if (c == 's') {
      dump_snap = strtoul(optarg, NULL, 0);
    } else if (c == 'f') {
      if (optarg == 0) {
        int i;
        printf("available file formats:\n");
        for (i = 0; i < WTAP_NUM_FILE_TYPES; i++) {
          if (wtap_dump_can_write_encap(i, WTAP_ENCAP_ETHERNET)) {
            printf("  %s: %s\n", wtap_file_type_short_string(i), wtap_file_type_string(i));
          }
        }
        exit(0);
      }
      dump_file_format = wtap_short_string_to_file_type(optarg);
    } else {
      printf("Unknown option: %c\n", c);
      usage();
    }
  }

  if (dump_file) {
    int err;
    file_dumper = wtap_dump_open(dump_file,
                                 dump_file_format,
                                 WTAP_ENCAP_ETHERNET,
                                 dump_snap,
                                 FALSE,
                                 &err);
    if (file_dumper == NULL) {
      switch (err) {
      case WTAP_ERR_UNSUPPORTED_FILE_TYPE:
        fprintf(stderr,
                "Capture files can't be written in format %i/%s.\n",
                dump_file_format, wtap_file_type_short_string(dump_file_format));
        break;
      case WTAP_ERR_UNSUPPORTED_ENCAP:
      case WTAP_ERR_ENCAP_PER_PACKET_UNSUPPORTED:
        fprintf(stderr,
                "The capture file being read can't be written in that format %i/%s.\n",
                dump_file_format, wtap_file_type_short_string(dump_file_format));
        break;
      case WTAP_ERR_CANT_OPEN:
        fprintf(stderr,
                "dump file %s couldn't be created for some unknown reason.\n",
                dump_file);
        break;
      case WTAP_ERR_SHORT_WRITE:
        fprintf(stderr,
                "A full header couldn't be written to dump file %s.\n",
                dump_file);
        break;
      default:
        fprintf(stderr,
                "Dump file %s could not be created.\n",
                dump_file);
        break;
      }
      return -1;
    }
  }

  snf_init(SNF_VERSION_API);
  rc = snf_open(boardnum, 1, NULL, dataring_sz, open_flags, &hsnf);
  if (rc) {
    errno = rc;
    perror("Can't open snf for sniffing");
    return -1;
  }
  rc = snf_ring_open(hsnf, &hring);
  if (rc) {
    errno = rc;
    perror("Can't open a receive ring for sniffing");
    return -1;
  }
  rc = snf_start(hsnf);
  if (rc) {
    errno = rc;
    perror("Can't start packet capture for sniffing");
    return -1;
  }

  printf("snf_recv ready to receive\n");

  if (SIG_ERR == signal(SIGINT, sigexit))
    exit(1);
  if (SIG_ERR == signal(SIGTERM, sigexit))
    exit(1);

  if (periodic_stats) {
    if (SIG_ERR == signal(SIGALRM, sigalrm))
      exit(1);
    itvl[itvl_idx].num_pkts = 0;
    itvl[itvl_idx].num_bytes = 0;
    gettimeofday(&itvl[itvl_idx].tv, NULL);
    itvl[itvl_idx].usecs = 0;
    itvl_idx = TOGGLE(itvl_idx);
    alarm(interval);
  }

  inspect_pkt = (verbose || (snap_print > 0));
  while (!finished && num_pkts < pkts_expected) {
    rc = snf_ring_recv(hring, timeout_ms, &recv_req);
    if (rc == EAGAIN || rc == EINTR)
      continue;
    else if (rc == 0) {
      num_pkts++;
      num_bytes += recv_req.length;
      if (dump_file) {

        /* todo:

           - rewrite completely (+ autoconf, etc.)

           - beware: i don't know when wiretap flushes its buffers, so
             the sniffer ring could loop (and overwrite data) *before*
             packet data has actually been written to disk

             sniffer doc says: Specifically, the Sniffer
             implementation assumes that with each successive call to
             snf_ring_recv within the context of a ring always means
             that the previous packet was consumed by the previous
             receive on that ring.

             one solution might be to have a ring size > wtap buffer
             size?

             OK -> seems to be ok since actually wtap does not buffer
             (only buffering is at the os level, but this is not a
             concern for us)

           - there may be a bug where first timestamps have 000
             nanoseconds. in snf, in wiretap, in my code?

           - check perfs and tweak

           - droits d'accès sur device myri: tout le monde peut s'en
             servir...

           - refaire les diags avec la carte débranchée

           - don't create capture file if we can open the myri devices
             (may need to reorder lot of things)
        */

        int err;

        work_wtap_pkthdr.ts.secs = recv_req.timestamp / 1000000000L;
        work_wtap_pkthdr.ts.nsecs = recv_req.timestamp % 1000000000L;
        work_wtap_pkthdr.caplen = (recv_req.length > dump_snap ? dump_snap : recv_req.length);
        work_wtap_pkthdr.len = recv_req.length;
        work_wtap_pkthdr.pkt_encap = WTAP_ENCAP_ETHERNET;

        if (! wtap_dump(file_dumper,
                        &work_wtap_pkthdr,
                        &work_wtap_pseudo_header,
                        (guchar *)recv_req.pkt_addr,
                        &err)) {
          show_dump_file_error(dump_file, err);
          finished = 1;
        }
      }
      if (!inspect_pkt)
        continue;
      if (verbose) {
        printf("pkt: %llu, len: %u\n", (unsigned long long)num_pkts,
            recv_req.length);
      }
      if (snap_print > 0) {
          dump_data(recv_req.pkt_addr,
                   (recv_req.length > snap_print) ? snap_print : recv_req.length);
      }
    }
    else {
      fprintf(stderr, "error: snf_recv = %d (%s)\n",
                 rc, strerror(rc));
      finished = 1;
    }
  }
  stats();
  if (file_dumper) {
    printf("wrote %lli bytes to %s.\n",
           wtap_get_bytes_dumped(file_dumper),
           dump_file);
  }
  printf("closing...\n");
  if (file_dumper) {
    int err;
    if (!wtap_dump_close(file_dumper, &err)) {
      show_dump_file_error(dump_file, err);
      return -1;
    }
  }
  rc = snf_ring_close(hring);
  if (rc) {
    errno = rc;
    perror("Can't close receive ring");
    return -1;
  }
  rc = snf_close(hsnf);
  if (rc) {
    errno = rc;
    perror("Can't close sniffer device");
    return -1;
  }
  printf("ok\n");
  return 0;
}

struct pkt_hdr {
  uint32_t length;
  uint32_t ofst;
};
typedef struct pkt_hdr pkt_hdr_t;
