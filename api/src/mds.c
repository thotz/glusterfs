#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>

#ifndef _CONFIG_H
#define _CONFIG_H
#include "config.h"
#endif

#include "glusterfs.h"
#include "logging.h"
#include "stack.h"
#include "event.h"
#include "glfs-mem-types.h"
#include "common-utils.h"
#include "syncop.h"
#include "call-stub.h"

#include "glfs.h"
#include "glfs-internal.h"
#include "hashfn.h"
#include "rpc-clnt.h"

/**
 * Converts pathinfo into hostname. 
 * Pathinfo usually contains dht information, hostname/ip
 * of the brick where data resides and path of the file in
 * that brick.Here the hostname is extracted from pathinfo
 * using basic string operations
 * input  : pathinfo
 * in/out : hostname
 * out    : ret
 * Returns zero and valid hostname on sucess 
 */

int
get_pathinfo_host (char *pathinfo, char *hostname, size_t size)
{
        /* Stores starting of hostname */
        char    *start = NULL;
        /* Stores ending of hostname */
        char    *end = NULL;
        int     ret  = -1;
        int     i    = 0;

        if (!pathinfo)
                goto out;

        start = strchr (pathinfo, ':');
        if (!start)
                goto out;
        start++;
        start = strchr (start,':');
         if (!start)
                goto out;
        end = strrchr (pathinfo, ':');
        if (start == end)
                goto out;

        memset (hostname, 0, size);
        i = 0;
        while (++start != end)
                hostname[i++] = *start;
        ret = 0;
out:
        return ret;
}

uint32_t
glfs_get_ds_addr (struct glfs *fs, struct glfs_object *object)
{
        gf_log (THIS->name , GF_LOG_TRACE ," reached get ds addr in libgfapi code");
        xlator_t        *subvol              = NULL;
        int             ret                  = 0;
        int             err                     ;
        loc_t           loc                  = {0, };
        dict_t          *dict                = NULL;
        char            *pathinfo            = NULL;
        char            hostname[1024]       = {0, };
        struct addrinfo hints, *res              ;
        struct in_addr  addr                 = {0, };

        loc.inode = object->inode;
        uuid_copy (loc.gfid, object->gfid);

        subvol = glfs_active_subvol (fs);
        if (!subvol) {
                gf_log (THIS->name , GF_LOG_ERROR , "No active Subvol");
                goto out;
        }
        ret = syncop_getxattr (subvol, &loc, &dict, GF_XATTR_PATHINFO_KEY);
        if (ret) {
              gf_log (THIS->name , GF_LOG_ERROR , "Cannot getxattr");
              goto out;
        }

        ret = dict_get_str (dict, GF_XATTR_PATHINFO_KEY, &pathinfo);
        if (ret) {
              gf_log (THIS->name , GF_LOG_ERROR , "cannot get pathinfo");
              goto out;
        }

        gf_log (THIS->name , GF_LOG_DEBUG , "pathinfo %s" , pathinfo);
        ret = get_pathinfo_host (pathinfo, hostname, sizeof (hostname));
        if (ret) {
              gf_log (THIS->name , GF_LOG_ERROR , "cannot get hostname");
              goto out;
        }
        gf_log(THIS->name,GF_LOG_DEBUG ,"hostname %s",hostname);
        memset(&hints, 0, sizeof(hints));
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = AF_INET;

        if ((err = getaddrinfo(hostname, NULL, &hints, &res)) != 0) {
              gf_log(THIS->name,GF_LOG_ERROR ,"error %d\n", err);
              goto out;
        }

        addr.s_addr = ((struct sockaddr_in *)(res->ai_addr))->sin_addr.s_addr;

        gf_log(THIS->name,GF_LOG_DEBUG ,"ip address : %s", inet_ntoa(addr));

        freeaddrinfo(res);
out:
        return addr.s_addr;
}

int glfs_get_file_layout (struct glfs *fs, struct glfs_object *object,
                          struct glfs_file_layout *gfl)
{
	gf_log (THIS->name , GF_LOG_TRACE ,"Entered function gluster_get_file_layout()...");

        gfl->devid = glfs_get_ds_addr(fs, object);

        if (gfl->devid == 0)
              return -1;
        /**
         * Currently we consider files are distributed among bricks,
         * not striped across.So the stripe unit shold be one and 
         * stripe type is dense which is represented by one.We use
         * dense stripe type because, commit happens through the 
         * meta-data-server
         */
        gfl->stripe_type = 1;
        gfl->stripe_unit = 1;

        return 0;
}
