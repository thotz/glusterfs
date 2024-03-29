/*
  Copyright (c) 2012 DataLab, s.l. <http://www.datalab.es>

  This file is part of the cluster/ec translator for GlusterFS.

  The cluster/ec translator for GlusterFS is free software: you can
  redistribute it and/or modify it under the terms of the GNU General
  Public License as published by the Free Software Foundation, either
  version 3 of the License, or (at your option) any later version.

  The cluster/ec translator for GlusterFS is distributed in the hope
  that it will be useful, but WITHOUT ANY WARRANTY; without even the
  implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
  PURPOSE. See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with the cluster/ec translator for GlusterFS. If not, see
  <http://www.gnu.org/licenses/>.
*/

#include "byte-order.h"

#include "ec-mem-types.h"
#include "ec-data.h"
#include "ec-helpers.h"
#include "ec-combine.h"
#include "ec-common.h"
#include "ec-fops.h"
#include "ec.h"

int32_t ec_child_valid(ec_t * ec, ec_fop_data_t * fop, int32_t idx)
{
    return (idx < ec->nodes) && (((fop->remaining >> idx) & 1) == 1);
}

int32_t ec_child_next(ec_t * ec, ec_fop_data_t * fop, int32_t idx)
{
    while (!ec_child_valid(ec, fop, idx))
    {
        if (++idx >= ec->nodes)
        {
            idx = 0;
        }
        if (idx == fop->first)
        {
            return -1;
        }
    }

    return idx;
}

uintptr_t ec_inode_good(inode_t * inode, xlator_t * xl)
{
    ec_inode_t * ctx;
    uintptr_t bad = 0;

    ctx = ec_inode_get(inode, xl);
    if (ctx != NULL)
    {
        bad = ctx->bad;
    }

    return ~bad;
}

uintptr_t ec_fd_good(fd_t * fd, xlator_t * xl)
{
    ec_fd_t * ctx;
    uintptr_t bad = 0;

    ctx = ec_fd_get(fd, xl);
    if ((ctx != NULL) && (ctx->loc.inode != NULL))
    {
        bad = ctx->bad;
    }

    return ~bad;
}

uintptr_t ec_update_inode(ec_fop_data_t * fop, inode_t * inode, uintptr_t good,
                          uintptr_t bad)
{
    ec_inode_t * ctx = NULL;

    if (inode != NULL)
    {
        LOCK(&inode->lock);

        ctx = __ec_inode_get(inode, fop->xl);
        if (ctx != NULL)
        {
            ctx->bad &= ~good;
            bad |= ctx->bad;
            ctx->bad = bad;
        }

        UNLOCK(&inode->lock);
    }

    return bad;
}

uintptr_t ec_update_fd(ec_fop_data_t * fop, fd_t * fd, uintptr_t good,
                       uintptr_t bad)
{
    ec_fd_t * ctx = NULL;

    LOCK(&fd->lock);

    ctx = __ec_fd_get(fd, fop->xl);
    if ((ctx != NULL) && (ctx->loc.inode != NULL))
    {
        ctx->bad &= ~good;
        bad |= ctx->bad;
        ctx->bad = bad;
    }

    UNLOCK(&fd->lock);

    return bad;
}

int32_t ec_heal_report(call_frame_t * frame, void * cookie, xlator_t * this,
                       int32_t op_ret, int32_t op_errno, uintptr_t mask,
                       uintptr_t good, uintptr_t bad, dict_t * xdata)
{
    if (op_ret < 0)
    {
        gf_log(this->name, GF_LOG_WARNING, "Heal failed (error %d)", op_errno);
    }
    else
    {
        gf_log(this->name, GF_LOG_INFO, "Heal succeeded on %d/%d subvolumes",
               ec_bits_count(mask & ~ (good | bad)),
               ec_bits_count(mask & ~good));
    }

    return 0;
}

void ec_check_status(ec_fop_data_t * fop)
{
    ec_t * ec = fop->xl->private;

    if ((ec->xl_up & ~(fop->remaining | fop->good)) == 0)
    {
        return;
    }

    gf_log(fop->xl->name, GF_LOG_WARNING, "Operation failed on some "
                                          "subvolumes (up=%lX, mask=%lX, "
                                          "remaining=%lX, good=%lX, bad=%lX)",
           ec->xl_up, fop->mask, fop->remaining, fop->good, fop->bad);

    if (fop->fd != NULL)
    {
        ec_fheal(fop->frame, fop->xl, -1, EC_MINIMUM_ONE, ec_heal_report, NULL,
                 fop->fd, NULL);
    }
    else
    {
        ec_heal(fop->frame, fop->xl, -1, EC_MINIMUM_ONE, ec_heal_report, NULL,
                &fop->loc[0], NULL);

        if (fop->loc[1].inode != NULL)
        {
            ec_heal(fop->frame, fop->xl, -1, EC_MINIMUM_ONE, ec_heal_report,
                    NULL, &fop->loc[1], NULL);
        }
    }
}

void ec_update_bad(ec_fop_data_t * fop, uintptr_t good)
{
    uintptr_t bad;
    int32_t update = 0;

    bad = fop->mask & ~(fop->remaining | good);
    if ((fop->bad & bad) != bad)
    {
        fop->bad |= bad;
        update = 1;
    }
    if ((fop->good & good) != good)
    {
        fop->good |= good;
        update = 1;
    }

    if (update && (fop->parent == NULL))
    {
        if ((fop->flags & EC_FLAG_UPDATE_LOC_PARENT) != 0)
        {
            ec_update_inode(fop, fop->loc[0].parent, good, bad);
        }
        if ((fop->flags & EC_FLAG_UPDATE_LOC_INODE) != 0)
        {
            ec_update_inode(fop, fop->loc[0].inode, good, bad);
        }
        ec_update_inode(fop, fop->loc[1].inode, good, bad);
        if ((fop->flags & EC_FLAG_UPDATE_FD_INODE) != 0)
        {
            ec_update_inode(fop, fop->fd->inode, good, bad);
        }
        if ((fop->flags & EC_FLAG_UPDATE_FD) != 0)
        {
            ec_update_fd(fop, fop->fd, good, bad);
        }

        ec_check_status(fop);
    }
}


void __ec_fop_set_error(ec_fop_data_t * fop, int32_t error)
{
    if ((error != 0) && (fop->error == 0))
    {
        fop->error = error;
    }
}

void ec_fop_set_error(ec_fop_data_t * fop, int32_t error)
{
    LOCK(&fop->lock);

    __ec_fop_set_error(fop, error);

    UNLOCK(&fop->lock);
}

int32_t ec_check_complete(ec_fop_data_t * fop, ec_resume_f resume)
{
    int32_t error = -1;

    LOCK(&fop->lock);

    GF_ASSERT(fop->resume == NULL);

    if (fop->jobs != 0)
    {
        ec_trace("WAIT", fop, "resume=%p", resume);

        fop->resume = resume;
    }
    else
    {
        error = fop->error;
        fop->error = 0;
    }

    UNLOCK(&fop->lock);

    return error;
}

void ec_wait_winds(ec_fop_data_t * fop)
{
    LOCK(&fop->lock);

    if (fop->winds > 0)
    {
        fop->jobs++;
        fop->refs++;

        fop->flags |= EC_FLAG_WAITING_WINDS;
    }

    UNLOCK(&fop->lock);
}

void ec_resume(ec_fop_data_t * fop, int32_t error)
{
    ec_resume_f resume = NULL;

    LOCK(&fop->lock);

    __ec_fop_set_error(fop, error);

    if (--fop->jobs == 0)
    {
        resume = fop->resume;
        fop->resume = NULL;
        if (resume != NULL)
        {
            ec_trace("RESUME", fop, "error=%d", error);

            if (fop->error != 0)
            {
                error = fop->error;
            }
            fop->error = 0;
        }
    }

    UNLOCK(&fop->lock);

    if (resume != NULL)
    {
        resume(fop, error);
    }

    ec_fop_data_release(fop);
}

void ec_resume_parent(ec_fop_data_t * fop, int32_t error)
{
    ec_fop_data_t * parent;

    parent = fop->parent;
    if (parent != NULL)
    {
        fop->parent = NULL;
        ec_resume(parent, error);
    }
}

void ec_complete(ec_fop_data_t * fop)
{
    ec_cbk_data_t * cbk = NULL;
    int32_t resume = 0;

    LOCK(&fop->lock);

    ec_trace("COMPLETE", fop, "");

    if (--fop->winds == 0)
    {
        if ((fop->answer == NULL) && (fop->expected != 1))
        {
            if (!list_empty(&fop->cbk_list))
            {
                cbk = list_entry(fop->cbk_list.next, ec_cbk_data_t, list);
                if ((cbk->count >= fop->minimum) &&
                    ((cbk->op_ret >= 0) || (cbk->op_errno != ENOTCONN)))
                {
                    fop->answer = cbk;

                    ec_update_bad(fop, cbk->mask);
                }
            }

            resume = 1;
        }
        else if ((fop->flags & EC_FLAG_WAITING_WINDS) != 0)
        {
            resume = 1;
        }
    }

    UNLOCK(&fop->lock);

    if (resume)
    {
        ec_resume(fop, 0);
    }

    ec_fop_data_release(fop);
}

int32_t ec_child_select(ec_fop_data_t * fop)
{
    ec_t * ec = fop->xl->private;
    uintptr_t mask = 0;
    int32_t first = 0, num = 0;

    fop->mask &= ec->node_mask;

    mask = ec->xl_up;
    if (fop->loc[0].inode != NULL)
    {
        mask |= ec_inode_good(fop->loc[0].inode, fop->xl);
    }
    if (fop->loc[1].inode != NULL)
    {
        mask |= ec_inode_good(fop->loc[1].inode, fop->xl);
    }
    if (fop->fd != NULL)
    {
        if (fop->fd->inode != NULL)
        {
            mask |= ec_inode_good(fop->fd->inode, fop->xl);
        }
        mask |= ec_fd_good(fop->fd, fop->xl);
    }
    if ((fop->mask & ~mask) != 0)
    {
        gf_log(fop->xl->name, GF_LOG_WARNING, "Executing operation with "
                                              "some subvolumes unavailable "
                                              "(%lX)", fop->mask & ~mask);

        fop->mask &= mask;
    }

    switch (fop->minimum)
    {
        case EC_MINIMUM_ALL:
            fop->minimum = ec_bits_count(fop->mask);
            if (fop->minimum >= ec->fragments)
            {
                break;
            }
        case EC_MINIMUM_MIN:
            fop->minimum = ec->fragments;
            break;
        case EC_MINIMUM_ONE:
            fop->minimum = 1;
    }

    first = ec->idx;
    if (++first >= ec->nodes)
    {
        first = 0;
    }
    ec->idx = first;

    fop->remaining = fop->mask;

    ec_trace("SELECT", fop, "");

    num = ec_bits_count(fop->mask);
    if ((num < fop->minimum) && (num < ec->fragments))
    {
        gf_log(ec->xl->name, GF_LOG_ERROR, "Insufficient available childs "
                                           "for this request (have %d, need "
                                           "%d)", num, fop->minimum);

        return 0;
    }

    LOCK(&fop->lock);

    fop->jobs++;
    fop->refs++;

    UNLOCK(&fop->lock);

    return 1;
}

int32_t ec_dispatch_next(ec_fop_data_t * fop, int32_t idx)
{
    ec_t * ec = fop->xl->private;

    LOCK(&fop->lock);

    idx = ec_child_next(ec, fop, idx);
    if (idx >= 0)
    {
        fop->remaining ^= 1ULL << idx;

        ec_trace("EXECUTE", fop, "idx=%d", idx);

        fop->winds++;
        fop->refs++;
    }

    UNLOCK(&fop->lock);

    if (idx >= 0)
    {
        fop->wind(ec, fop, idx);
    }

    return idx;
}

void ec_dispatch_mask(ec_fop_data_t * fop, uintptr_t mask)
{
    ec_t * ec = fop->xl->private;
    int32_t count, idx;

    count = ec_bits_count(mask);

    LOCK(&fop->lock);

    ec_trace("EXECUTE", fop, "mask=%lX", mask);

    fop->remaining ^= mask;

    fop->winds += count;
    fop->refs += count;

    UNLOCK(&fop->lock);

    idx = 0;
    while (mask != 0)
    {
        if ((mask & 1) != 0)
        {
            fop->wind(ec, fop, idx);
        }
        idx++;
        mask >>= 1;
    }
}

void ec_dispatch_start(ec_fop_data_t * fop)
{
    fop->answer = NULL;
    fop->good = 0;
    fop->bad = 0;

    INIT_LIST_HEAD(&fop->cbk_list);

    if (fop->lock_count > 0)
    {
        ec_owner_copy(fop->frame, &fop->req_frame->root->lk_owner);
    }
}

void ec_dispatch_one(ec_fop_data_t * fop)
{
    ec_t * ec = fop->xl->private;

    ec_dispatch_start(fop);

    if (ec_child_select(fop))
    {
        fop->expected = 1;
        fop->first = ec->idx;

        ec_dispatch_next(fop, fop->first);
    }
}

int32_t ec_dispatch_one_retry(ec_fop_data_t * fop, int32_t idx, int32_t op_ret,
                              int32_t op_errno)
{
    if ((op_ret < 0) && (op_errno == ENOTCONN))
    {
        return (ec_dispatch_next(fop, idx) >= 0);
    }

    return 0;
}

void ec_dispatch_inc(ec_fop_data_t * fop)
{
    ec_dispatch_start(fop);

    if (ec_child_select(fop))
    {
        fop->expected = ec_bits_count(fop->remaining);
        fop->first = 0;

        ec_dispatch_next(fop, 0);
    }
}

void ec_dispatch_all(ec_fop_data_t * fop)
{
    ec_dispatch_start(fop);

    if (ec_child_select(fop))
    {
        fop->expected = ec_bits_count(fop->remaining);
        fop->first = 0;

        ec_dispatch_mask(fop, fop->remaining);
    }
}

void ec_dispatch_min(ec_fop_data_t * fop)
{
    ec_t * ec = fop->xl->private;
    uintptr_t mask;
    int32_t idx, count;

    ec_dispatch_start(fop);

    if (ec_child_select(fop))
    {
        fop->expected = count = ec->fragments;
        fop->first = ec->idx;
        idx = fop->first - 1;
        mask = 0;
        while (count-- > 0)
        {
            idx = ec_child_next(ec, fop, idx + 1);
            mask |= 1ULL << idx;
        }

        ec_dispatch_mask(fop, mask);
    }
}

ec_lock_t * ec_lock_allocate(xlator_t * xl, int32_t kind, loc_t * loc)
{
    ec_t * ec = xl->private;
    ec_lock_t * lock;

    if ((loc->inode == NULL) ||
        (uuid_is_null(loc->gfid) && uuid_is_null(loc->inode->gfid)))
    {
        gf_log(xl->name, GF_LOG_ERROR, "Trying to lock based on an invalid "
                                       "inode");

        return NULL;
    }

    lock = mem_get0(ec->lock_pool);
    if (lock != NULL)
    {
        lock->kind = kind;
        lock->good_mask = -1ULL;
        INIT_LIST_HEAD(&lock->waiting);
        if (!ec_loc_from_loc(xl, &lock->loc, loc))
        {
            mem_put(lock);
            lock = NULL;
        }
    }

    return lock;
}

void ec_lock_destroy(ec_lock_t * lock)
{
    GF_FREE(lock->basename);
    loc_wipe(&lock->loc);

    mem_put(lock);
}

int32_t ec_lock_compare(ec_lock_t * lock1, ec_lock_t * lock2)
{
    int32_t res;

    res = uuid_compare(lock1->loc.gfid, lock2->loc.gfid);
    if (res != 0)
    {
        return res;
    }
    if (lock1->basename == NULL)
    {
        if (lock2->basename == NULL)
        {
            return 0;
        }
        return 1;
    }
    if (lock2->basename == NULL)
    {
        return -1;
    }
    return strcmp(lock1->basename, lock2->basename);
}

void ec_lock_insert(ec_fop_data_t * fop, ec_lock_t * lock)
{
    ec_lock_t * tmp;

    if ((fop->lock_count > 0) &&
        (ec_lock_compare(fop->locks[0].lock, lock) > 0))
    {
        tmp = fop->locks[0].lock;
        fop->locks[0].lock = lock;
        lock = tmp;
    }
    fop->locks[fop->lock_count].lock = lock;
    fop->locks[fop->lock_count].fop = fop;
    fop->lock_count++;

    lock->refs++;
}

void ec_lock_prepare_entry(ec_fop_data_t * fop, loc_t * loc)
{
    ec_lock_t * lock = NULL;
    ec_inode_t * ctx = NULL;
    char * name = NULL;
    loc_t tmp;
    int32_t error;

    if ((fop->parent != NULL) || (fop->error != 0))
    {
        return;
    }

    error = ec_loc_parent(fop->xl, loc, &tmp, &name);
    if (error != 0)
    {
        ec_fop_set_error(fop, error);

        return;
    }

    LOCK(&tmp.inode->lock);

    ctx = __ec_inode_get(tmp.inode, fop->xl);
    if (ctx == NULL)
    {
        __ec_fop_set_error(fop, EIO);

        goto unlock;
    }

    list_for_each_entry(lock, &ctx->entry_locks, list)
    {
        if (strcmp(lock->basename, name) == 0)
        {
            ec_trace("LOCK_ENTRYLK", fop, "lock=%p, inode=%p, path=%s, "
                                          "name=%s. Lock already acquired",
                     lock, tmp.inode, tmp.path, name);

            goto insert;
        }
    }

    lock = ec_lock_allocate(fop->xl, EC_LOCK_ENTRY, &tmp);
    if (lock == NULL)
    {
        __ec_fop_set_error(fop, EIO);

        goto unlock;
    }

    ec_trace("LOCK_CREATE", fop, "lock=%p", lock);

    lock->type = ENTRYLK_WRLCK;
    lock->basename = name;
    name = NULL;

    list_add_tail(&lock->list, &ctx->entry_locks);

insert:
    ec_lock_insert(fop, lock);

unlock:
    UNLOCK(&tmp.inode->lock);

    loc_wipe(&tmp);
    GF_FREE(name);
}

void ec_lock_prepare_inode(ec_fop_data_t * fop, loc_t * loc)
{
    ec_lock_t * lock;
    ec_inode_t * ctx;

    if ((fop->parent != NULL) || (fop->error != 0) || (loc->inode == NULL))
    {
        return;
    }

    LOCK(&loc->inode->lock);

    ctx = __ec_inode_get(loc->inode, fop->xl);
    if (ctx == NULL)
    {
        __ec_fop_set_error(fop, EIO);

        goto unlock;
    }

    if (!list_empty(&ctx->inode_locks))
    {
        lock = list_entry(ctx->inode_locks.next, ec_lock_t, list);
        ec_trace("LOCK_INODELK", fop, "lock=%p, inode=%p. Lock already "
                                      "acquired", lock, loc->inode);

        goto insert;
    }

    lock = ec_lock_allocate(fop->xl, EC_LOCK_INODE, loc);
    if (lock == NULL)
    {
        __ec_fop_set_error(fop, EIO);

        goto unlock;
    }

    ec_trace("LOCK_CREATE", fop, "lock=%p", lock);

    lock->flock.l_type = F_WRLCK;
    lock->flock.l_whence = SEEK_SET;

    list_add_tail(&lock->list, &ctx->inode_locks);

insert:
    ec_lock_insert(fop, lock);

unlock:
    UNLOCK(&loc->inode->lock);
}

void ec_lock_prepare_fd(ec_fop_data_t * fop, fd_t * fd)
{
    loc_t loc;

    if ((fop->parent != NULL) || (fop->error != 0))
    {
        return;
    }

    if (ec_loc_from_fd(fop->xl, &loc, fd))
    {
        ec_lock_prepare_inode(fop, &loc);

        loc_wipe(&loc);
    }
    else
    {
        ec_fop_set_error(fop, EIO);
    }
}

int32_t ec_locked(call_frame_t * frame, void * cookie, xlator_t * this,
                  int32_t op_ret, int32_t op_errno, dict_t * xdata)
{
    ec_fop_data_t * fop = cookie;
    ec_lock_t * lock = NULL;

    if (op_ret >= 0)
    {
        lock = fop->data;
        lock->mask = fop->good;
        lock->acquired = 1;

        fop->parent->mask &= fop->good;
        fop->parent->locked++;

        ec_trace("LOCKED", fop->parent, "lock=%p", lock);

        ec_lock(fop->parent);
    }
    else
    {
        gf_log(this->name, GF_LOG_WARNING, "Failed to complete preop lock");
    }

    return 0;
}

void ec_lock(ec_fop_data_t * fop)
{
    ec_lock_t * lock;

    while (fop->locked < fop->lock_count)
    {
        lock = fop->locks[fop->locked].lock;

        LOCK(&lock->loc.inode->lock);

        if (lock->owner != NULL)
        {
            ec_trace("LOCK_WAIT", fop, "lock=%p", lock);

            list_add_tail(&fop->locks[fop->locked].wait_list, &lock->waiting);

            fop->jobs++;
            fop->refs++;

            UNLOCK(&lock->loc.inode->lock);

            break;
        }
        lock->owner = fop;

        UNLOCK(&lock->loc.inode->lock);

        if (!lock->acquired)
        {
            ec_owner_set(fop->frame, lock);

            if (lock->kind == EC_LOCK_ENTRY)
            {
                ec_trace("LOCK_ACQUIRE", fop, "lock=%p, inode=%p, path=%s, "
                         "name=%s", lock, lock->loc.inode, lock->loc.path,
                         lock->basename);

                ec_entrylk(fop->frame, fop->xl, -1, EC_MINIMUM_ALL, ec_locked,
                           lock, fop->xl->name, &lock->loc, lock->basename,
                           ENTRYLK_LOCK, lock->type, NULL);
            }
            else
            {
                ec_trace("LOCK_ACQUIRE", fop, "lock=%p, inode=%p", lock,
                         lock->loc.inode);

                ec_inodelk(fop->frame, fop->xl, -1, EC_MINIMUM_ALL, ec_locked,
                           lock, fop->xl->name, &lock->loc, F_SETLKW,
                           &lock->flock, NULL);
            }

            break;
        }

        ec_trace("LOCK_REUSE", fop, "lock=%p", lock);

        if (lock->have_size)
        {
            fop->pre_size = fop->post_size = lock->size;
            fop->have_size = 1;
        }
        fop->mask &= lock->good_mask;

        fop->locked++;
    }
}

int32_t ec_unlocked(call_frame_t * frame, void * cookie, xlator_t * this,
                    int32_t op_ret, int32_t op_errno, dict_t * xdata)
{
    ec_fop_data_t * fop = cookie;

    if (op_ret < 0)
    {
        gf_log(this->name, GF_LOG_WARNING, "entry/inode unlocking failed (%s)",
               ec_fop_name(fop->parent->id));
    }
    else
    {
        ec_trace("UNLOCKED", fop->parent, "lock=%p", fop->data);
    }

    return 0;
}

void ec_unlock(ec_fop_data_t * fop)
{
    ec_lock_t * lock;
    int32_t i, refs;

    for (i = 0; i < fop->lock_count; i++)
    {
        lock = fop->locks[i].lock;

        LOCK(&lock->loc.inode->lock);

        ec_trace("UNLOCK", fop, "lock=%p", lock);

        refs = --lock->refs;
        if (refs == 0)
        {
            list_del_init(&lock->list);
        }

        UNLOCK(&lock->loc.inode->lock);

        if (refs == 0)
        {
            if (lock->mask != 0)
            {
                ec_owner_set(fop->frame, lock);

                switch (lock->kind)
                {
                    case EC_LOCK_ENTRY:
                        ec_trace("UNLOCK_ENTRYLK", fop, "lock=%p, inode=%p, "
                                                        "path=%s, basename=%s",
                                 lock, lock->loc.inode, lock->loc.path,
                                 lock->basename);

                        ec_entrylk(fop->frame, fop->xl, lock->mask,
                                   EC_MINIMUM_ALL, ec_unlocked, lock,
                                   fop->xl->name, &lock->loc, lock->basename,
                                   ENTRYLK_UNLOCK, lock->type, NULL);

                        break;

                    case EC_LOCK_INODE:
                        lock->flock.l_type = F_UNLCK;
                        ec_trace("UNLOCK_INODELK", fop, "lock=%p, inode=%p",
                                 lock, lock->loc.inode);

                        ec_inodelk(fop->frame, fop->xl, lock->mask,
                                   EC_MINIMUM_ALL, ec_unlocked, lock,
                                   fop->xl->name, &lock->loc, F_SETLK,
                                   &lock->flock, NULL);

                        break;

                    default:
                        gf_log(fop->xl->name, GF_LOG_ERROR, "Invalid lock "
                                                            "type");
                }
            }

            ec_trace("LOCK_DESTROY", fop, "lock=%p", lock);

            ec_lock_destroy(lock);
        }
    }
}

int32_t ec_get_size_version_set(call_frame_t * frame, void * cookie,
                                xlator_t * this, int32_t op_ret,
                                int32_t op_errno, inode_t * inode,
                                struct iatt * buf, dict_t * xdata,
                                struct iatt * postparent)
{
    ec_fop_data_t * fop = cookie;
    ec_inode_t * ctx;
    ec_lock_t * lock;

    if (op_ret >= 0)
    {
        LOCK(&inode->lock);

        ctx = __ec_inode_get(inode, this);
        if ((ctx != NULL) && !list_empty(&ctx->inode_locks))
        {
            lock = list_entry(ctx->inode_locks.next, ec_lock_t, list);

            lock->have_size = 1;
            lock->size = buf->ia_size;
            lock->version = fop->answer->version;
        }

        UNLOCK(&inode->lock);

        if (lock != NULL)
        {
            // Only update parent mask if the lookup has been made with
            // inode locked.
            fop->parent->mask &= fop->good;
        }

        fop->parent->pre_size = fop->parent->post_size = buf->ia_size;

        fop->parent->have_size = 1;

    }
    else
    {
        gf_log(this->name, GF_LOG_WARNING, "Failed to get size and version "
                                           "(error %d)", op_errno);
        ec_fop_set_error(fop, op_errno);
    }

    return 0;
}

void ec_get_size_version(ec_fop_data_t * fop)
{
    loc_t loc;
    dict_t * xdata;
    uid_t uid;
    gid_t gid;
    int32_t error = ENOMEM;

    if (fop->have_size)
    {
        return;
    }

    if ((fop->parent != NULL) && fop->parent->have_size)
    {
        fop->pre_size = fop->parent->pre_size;
        fop->post_size = fop->parent->post_size;

        fop->have_size = 1;

        return;
    }

    memset(&loc, 0, sizeof(loc));

    xdata = dict_new();
    if (xdata == NULL)
    {
        goto out;
    }
    if ((dict_set_uint64(xdata, EC_XATTR_VERSION, 0) != 0) ||
        (dict_set_uint64(xdata, EC_XATTR_SIZE, 0) != 0))
    {
        goto out;
    }

    uid = fop->frame->root->uid;
    gid = fop->frame->root->gid;

    fop->frame->root->uid = 0;
    fop->frame->root->gid = 0;

    error = EIO;

    if (fop->fd == NULL)
    {
        if (!ec_loc_from_loc(fop->xl, &loc, &fop->loc[0]))
        {
            goto out;
        }
        if (uuid_is_null(loc.pargfid))
        {
            if (loc.parent != NULL)
            {
                inode_unref(loc.parent);
                loc.parent = NULL;
            }
            GF_FREE((char *)loc.path);
            loc.path = NULL;
            loc.name = NULL;
        }
    }
    else if (!ec_loc_from_fd(fop->xl, &loc, fop->fd))
    {
        goto out;
    }

    ec_lookup(fop->frame, fop->xl, fop->mask, EC_MINIMUM_MIN,
              ec_get_size_version_set, NULL, &loc, xdata);

    fop->frame->root->uid = uid;
    fop->frame->root->gid = gid;

    error = 0;

out:
    loc_wipe(&loc);

    if (xdata != NULL)
    {
        dict_unref(xdata);
    }

    ec_fop_set_error(fop, error);
}

int32_t ec_update_size_version_done(call_frame_t * frame, void * cookie,
                                    xlator_t * this, int32_t op_ret,
                                    int32_t op_errno, dict_t * xattr,
                                    dict_t * xdata)
{
    ec_fop_data_t * fop = cookie;

    if (op_ret < 0)
    {
        gf_log(fop->xl->name, GF_LOG_ERROR, "Failed to update version and "
                                            "size (error %d)", op_errno);
    }
    else
    {
        fop->parent->mask &= fop->good;
    }

    return 0;
}

void ec_update_size_version(ec_fop_data_t * fop, uint64_t version,
                            uint64_t size)
{
    dict_t * dict;
    uid_t uid;
    gid_t gid;

    if (fop->parent != NULL)
    {
        fop->parent->post_size = fop->post_size;

        return;
    }

    ec_trace("UPDATE", fop, "version=%ld, size=%ld", version, size);

    dict = dict_new();
    if (dict == NULL)
    {
        goto out;
    }

    if (ec_dict_set_number(dict, EC_XATTR_VERSION, version) != 0)
    {
        goto out;
    }
    if (size != 0)
    {
        if (ec_dict_set_number(dict, EC_XATTR_SIZE, size) != 0)
        {
            goto out;
        }
    }

    uid = fop->frame->root->uid;
    gid = fop->frame->root->gid;

    fop->frame->root->uid = 0;
    fop->frame->root->gid = 0;

    if (fop->fd == NULL)
    {
        ec_xattrop(fop->frame, fop->xl, fop->mask, EC_MINIMUM_MIN,
                   ec_update_size_version_done, NULL, &fop->loc[0],
                   GF_XATTROP_ADD_ARRAY64, dict, NULL);
    }
    else
    {
        ec_fxattrop(fop->frame, fop->xl, fop->mask, EC_MINIMUM_MIN,
                    ec_update_size_version_done, NULL, fop->fd,
                    GF_XATTROP_ADD_ARRAY64, dict, NULL);
    }

    fop->frame->root->uid = uid;
    fop->frame->root->gid = gid;

    dict_unref(dict);

    return;

out:
    if (dict != NULL)
    {
        dict_unref(dict);
    }

    ec_fop_set_error(fop, EIO);

    gf_log(fop->xl->name, GF_LOG_ERROR, "Unable to update version and size");
}

void ec_flush_size_version(ec_fop_data_t * fop)
{
    ec_lock_t * lock;
    uint64_t version;
    size_t delta;

    GF_ASSERT(fop->lock_count == 1);

    lock = fop->locks[0].lock;

    GF_ASSERT(lock->kind == EC_LOCK_INODE);

    LOCK(&lock->loc.inode->lock);

    GF_ASSERT(lock->owner == fop);

    version = lock->version_delta;
    delta = lock->size_delta;
    lock->version_delta = 0;
    lock->size_delta = 0;

    UNLOCK(&lock->loc.inode->lock);

    if (version > 0)
    {
        ec_update_size_version(fop, version, delta);
    }
}

void ec_lock_reuse(ec_fop_data_t * fop, int32_t update)
{
    ec_fop_data_t * wait_fop;
    ec_lock_t * lock;
    ec_lock_link_t * link;
    size_t delta = 0;
    uint64_t version = 0;
    int32_t refs = 0;
    int32_t i;

    for (i = 0; i < fop->lock_count; i++)
    {
        wait_fop = NULL;

        lock = fop->locks[i].lock;

        LOCK(&lock->loc.inode->lock);

        ec_trace("LOCK_DONE", fop, "lock=%p", lock);

        GF_ASSERT(lock->owner == fop);
        lock->owner = NULL;

        if (lock->kind == EC_LOCK_INODE)
        {
            if (update && (fop->error == 0))
            {
                lock->version_delta++;
                lock->size_delta += fop->post_size - fop->pre_size;
            }
            version = lock->version_delta;
            delta = lock->size_delta;
            refs = lock->refs;
            if (refs == 1)
            {
                lock->version_delta = 0;
                lock->size_delta = 0;
            }

            if (fop->have_size)
            {
                lock->size = fop->post_size;
                lock->have_size = 1;
            }
        }
        lock->good_mask &= fop->mask;

        if (!list_empty(&lock->waiting))
        {
            link = list_entry(lock->waiting.next, ec_lock_link_t, wait_list);
            list_del_init(&link->wait_list);

            wait_fop = link->fop;

            if (lock->kind == EC_LOCK_INODE)
            {
                wait_fop->pre_size = wait_fop->post_size = fop->post_size;
                wait_fop->have_size = fop->have_size;
            }
            wait_fop->mask &= fop->mask;
        }

        UNLOCK(&lock->loc.inode->lock);

        if (wait_fop != NULL)
        {
            ec_lock(wait_fop);

            ec_resume(wait_fop, 0);
        }
    }

    if ((refs == 1) && (version > 0))
    {
        ec_update_size_version(fop, version, delta);
    }
}

void __ec_manager(ec_fop_data_t * fop, int32_t error)
{
    do
    {
        ec_trace("MANAGER", fop, "error=%d", error);

        if (fop->state == EC_STATE_END)
        {
            ec_fop_data_release(fop);

            break;
        }

        if (error != 0)
        {
            fop->error = error;
            fop->state = -fop->state;
        }

        fop->state = fop->handler(fop, fop->state);

        error = ec_check_complete(fop, __ec_manager);
    } while (error >= 0);
}

void ec_manager(ec_fop_data_t * fop, int32_t error)
{
    GF_ASSERT(fop->jobs == 0);
    GF_ASSERT(fop->winds == 0);
    GF_ASSERT(fop->error == 0);

    if (fop->state == EC_STATE_START)
    {
        fop->state = EC_STATE_INIT;
    }

    __ec_manager(fop, error);
}
