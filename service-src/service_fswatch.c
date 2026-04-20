#include "skynet.h"
#include "skynet_socket.h"

#include <sys/stat.h>
#include <dirent.h>
#include <limits.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>

#ifdef __linux__
#include <sys/inotify.h>

struct fswatch_watch {
	int wd;					// inotify watch descriptor
	char *path;				// monitored path (full path)
	bool is_dir;			// is this a directory
	bool recursive;			// whether to monitor recursively
	struct fswatch_watch *parent;	// parent for recursive watches
	struct fswatch_watch *next;		// linked list
};

struct fswatch_service {
	struct skynet_context *ctx;
	int inotify_fd;			// inotify file descriptor
	int socket_id;			// Skynet bound socket id
	uint32_t subscriber;	// subscriber service handle
	struct fswatch_watch *watches;	// linked list of all watches
	unsigned char *partial_buffer;	// buffer for partial event
	size_t partial_len;		// length of partial data in buffer
	size_t partial_cap;		// capacity of partial buffer
};

// Find watch by watch descriptor
static struct fswatch_watch *
find_watch(struct fswatch_service *is, int wd) {
	struct fswatch_watch *w = is->watches;
	while (w) {
		if (w->wd == wd)
			return w;
		w = w->next;
	}
	return NULL;
}

// Add watch to linked list
static void
add_watch(struct fswatch_service *is, struct fswatch_watch *w) {
	w->next = is->watches;
	is->watches = w;
}

// Remove watch from linked list
static void
remove_watch(struct fswatch_service *is, struct fswatch_watch *w) {
	struct fswatch_watch **p = &is->watches;
	while (*p) {
		if (*p == w) {
			*p = w->next;
			return;
		}
		p = &(*p)->next;
	}
}

// Recursively remove all child watches
static void
remove_child_watches(struct fswatch_service *is, struct fswatch_watch *parent) {
	struct fswatch_watch *w = is->watches;
	while (w) {
		struct fswatch_watch *next = w->next;
		if (w->parent == parent) {
			remove_child_watches(is, w);
			remove_watch(is, w);
			inotify_rm_watch(is->inotify_fd, w->wd);
			skynet_free(w->path);
			skynet_free(w);
		}
		w = next;
	}
}

// Add a single path to inotify
static struct fswatch_watch *
add_watch_path(struct fswatch_service *is, const char *path, bool recursive, struct fswatch_watch *parent) {
	struct stat st;
	if (stat(path, &st) == -1) {
		skynet_error(is->ctx, "fswatch: cannot stat %s: %s", path, strerror(errno));
		return NULL;
	}

	uint32_t mask = IN_MODIFY | IN_CREATE | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE_SELF | IN_ATTRIB;
	struct fswatch_watch *w = skynet_malloc(sizeof(*w));
	w->path = skynet_malloc(strlen(path) + 1);
	strcpy(w->path, path);
	w->is_dir = S_ISDIR(st.st_mode);
	w->recursive = recursive;
	w->parent = parent;
	w->next = NULL;

	w->wd = inotify_add_watch(is->inotify_fd, path, mask);
	if (w->wd == -1) {
		skynet_error(is->ctx, "fswatch: cannot add watch %s: %s", path, strerror(errno));
		skynet_free(w->path);
		skynet_free(w);
		return NULL;
	}

	add_watch(is, w);
	return w;
}

// Recursively add all subdirectories
static void
add_subdirectories(struct fswatch_service *is, const char *dirpath, struct fswatch_watch *parent) {
	DIR *dir = opendir(dirpath);
	if (!dir) {
		skynet_error(is->ctx, "fswatch: cannot open directory %s: %s", dirpath, strerror(errno));
		return;
	}

	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		char fullpath[PATH_MAX];
		int len = strlen(dirpath);
		if (len + strlen(entry->d_name) + 2 > PATH_MAX) {
			skynet_error(is->ctx, "fswatch: path too long: %s/%s", dirpath, entry->d_name);
			continue;
		}
		sprintf(fullpath, "%s/%s", dirpath, entry->d_name);

		struct stat st;
		if (stat(fullpath, &st) == -1)
			continue;

		if (S_ISDIR(st.st_mode)) {
			struct fswatch_watch *child = add_watch_path(is, fullpath, true, parent);
			if (child) {
				add_subdirectories(is, fullpath, child);
			}
		}
	}

	closedir(dir);
}

// Handle add command
static void
_cmd_add(struct fswatch_service *is, char *args) {
	bool recursive = false;
	char *path = args;

	if (strncmp(args, "-r ", 3) == 0) {
		recursive = true;
		path = args + 3;
	} else if (strncmp(args, "-r", 2) == 0) {
		// just "-r" without path, invalid
		skynet_error(is->ctx, "fswatch: invalid add command: missing path");
		return;
	}

	// Skip leading whitespace
	while (*path == ' ')
		path++;

	if (*path == '\0') {
		skynet_error(is->ctx, "fswatch: invalid add command: missing path");
		return;
	}

	struct fswatch_watch *w = add_watch_path(is, path, recursive, NULL);
	if (!w) {
		// error already logged
		return;
	}

	// If it's a directory and recursive is on, add all subdirectories
	if (w->is_dir && w->recursive) {
		add_subdirectories(is, path, w);
	}

	// Return the watch descriptor to the caller if we have a subscriber
	if (is->subscriber != 0) {
		char resp[64];
		int n = sprintf(resp, "added %d", w->wd);
		skynet_send(is->ctx, 0, is->subscriber, PTYPE_TEXT, 0, resp, n);
	}
}

// Handle remove command
static void
_cmd_remove(struct fswatch_service *is, char *args) {
	int wd = atoi(args);
	struct fswatch_watch *w = find_watch(is, wd);
	if (!w) {
		skynet_error(is->ctx, "fswatch: cannot find watch %d", wd);
		return;
	}

	// Remove all children recursively
	remove_child_watches(is, w);

	// Remove the watch itself
	remove_watch(is, w);
	inotify_rm_watch(is->inotify_fd, w->wd);
	skynet_free(w->path);
	skynet_free(w);
}

// Handle subscribe command
static void
_cmd_subscribe(struct fswatch_service *is, uint32_t source) {
	is->subscriber = source;
}

// Parse incoming text command
static void
_ctrl(struct fswatch_service *is, const void *msg, int sz) {
	char tmp[sz + 1];
	memcpy(tmp, msg, sz);
	tmp[sz] = '\0';

	char *command = tmp;
	int i;
	for (i = 0; i < sz; i++) {
		if (command[i] == ' ')
			break;
	}

	if (memcmp(command, "add", i) == 0) {
		char *args = command + i;
		while (*args == ' ')
			args++;
		_cmd_add(is, args);
		return;
	}

	if (memcmp(command, "remove", i) == 0) {
		char *args = command + i;
		while (*args == ' ')
			args++;
		_cmd_remove(is, args);
		return;
	}

	if (memcmp(command, "subscribe", i) == 0) {
		// source is the subscriber, handled in callback
		return;
	}

	skynet_error(is->ctx, "fswatch: Unknown command: %s", command);
}

// Send event to subscriber
static void
send_event(struct fswatch_service *is, struct fswatch_watch *w, struct inotify_event *event) {
	if (is->subscriber == 0)
		return;

	char msg[512];
	int len;
	if (event->name[0] == '\0') {
		len = sprintf(msg, "event %d %08x", w->wd, (unsigned int)event->mask);
	} else {
		len = sprintf(msg, "event %d %08x %s", w->wd, (unsigned int)event->mask, event->name);
	}
	skynet_send(is->ctx, 0, is->subscriber, PTYPE_TEXT, 0, msg, len);

	// If a directory was created and parent is recursive, automatically add it
	if ((event->mask & IN_CREATE) && event->mask & IN_ISDIR && w->recursive) {
		// event->name is the new directory name relative to w->path
		char fullpath[PATH_MAX];
		int len = strlen(w->path);
		if (len + strlen(event->name) + 2 > PATH_MAX) {
			skynet_error(is->ctx, "fswatch: path too long: %s/%s", w->path, event->name);
		} else {
			sprintf(fullpath, "%s/%s", w->path, event->name);
			struct fswatch_watch *new_watch = add_watch_path(is, fullpath, true, w);
			// For recursive add, we also need to add its subdirectories
			if (new_watch) {
				add_subdirectories(is, fullpath, new_watch);
			}
		}
	}

	// If directory was deleted, remove it from our list
	if (event->mask & IN_DELETE_SELF) {
		remove_child_watches(is, w);
		remove_watch(is, w);
		inotify_rm_watch(is->inotify_fd, w->wd);
		skynet_free(w->path);
		skynet_free(w);
	}
}

// Handle inotify events from socket
static void
dispatch_events(struct fswatch_service *is, struct skynet_socket_message *message, int sz) {
	if (message->type != SKYNET_SOCKET_TYPE_DATA)
		return;

	char *new_data = (char *)message->buffer;
	size_t new_len = message->ud;

	// If we have partial data from previous delivery, combine it with new data
	size_t total_len = is->partial_len + new_len;
	unsigned char *total_buffer;

	if (is->partial_len > 0) {
		// Need to expand buffer if necessary
		if (total_len > is->partial_cap) {
			size_t new_cap = total_len > is->partial_cap * 2 ? total_len : is->partial_cap * 2;
			unsigned char *new_buf = skynet_malloc(new_cap);
			memcpy(new_buf, is->partial_buffer, is->partial_len);
			skynet_free(is->partial_buffer);
			is->partial_buffer = new_buf;
			is->partial_cap = new_cap;
		}
		// Append new data to partial buffer
		memcpy(is->partial_buffer + is->partial_len, new_data, new_len);
		total_buffer = is->partial_buffer;
		is->partial_len = total_len;
	} else {
		// No previous partial data, process directly
		total_buffer = (unsigned char *)new_data;
		is->partial_len = total_len;
		if (total_len > is->partial_cap) {
			long page_size = sysconf(_SC_PAGESIZE);
			if (page_size < 0) page_size = 4096;
			is->partial_cap = total_len > (size_t)page_size ? total_len : (size_t)page_size;
			is->partial_buffer = skynet_malloc(is->partial_cap);
		}
		memcpy(is->partial_buffer, new_data, new_len);
	}

	size_t i = 0;
	while (i + sizeof(struct inotify_event) <= is->partial_len) {
		struct inotify_event *event = (struct inotify_event *)&total_buffer[i];
		size_t event_size = sizeof(struct inotify_event) + event->len;
		// Check if we have the complete event
		if (i + event_size > is->partial_len) {
			// Partial event, break and keep it in buffer
			break;
		}
		// Process the complete event
		struct fswatch_watch *w = find_watch(is, event->wd);
		if (w) {
			send_event(is, w, event);
		}
		i += event_size;
	}

	// If there's remaining partial data, keep it for next time
	if (i < is->partial_len) {
		// Move remaining data to the beginning of buffer
		memmove(is->partial_buffer, is->partial_buffer + i, is->partial_len - i);
		is->partial_len -= i;
	} else {
		// All data processed, reset
		is->partial_len = 0;
	}
}

// Main callback
static int
_inotify_cb(struct skynet_context * ctx, void * ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	struct fswatch_service *is = ud;
	switch(type) {
	case PTYPE_TEXT:
		_ctrl(is, msg, (int)sz);
		if (memcmp((const char *)msg, "subscribe", 9) == 0) {
			_cmd_subscribe(is, source);
		}
		break;
	case PTYPE_SOCKET:
		dispatch_events(is, (struct skynet_socket_message *)msg,
			(int)(sz - sizeof(struct skynet_socket_message)));
		break;
	}
	return 0;
}

// Create service instance
struct fswatch_service *
fswatch_create(void) {
	struct fswatch_service *is = skynet_malloc(sizeof(*is));
	memset(is, 0, sizeof(*is));
	is->inotify_fd = -1;
	is->socket_id = -1;
	is->partial_buffer = NULL;
	is->partial_len = 0;
	is->partial_cap = 0;
	return is;
}

// Initialize service
int
fswatch_init(struct fswatch_service *is, struct skynet_context *ctx, const char *parm) {
	is->ctx = ctx;

	// Initialize inotify
	is->inotify_fd = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
	if (is->inotify_fd == -1) {
		skynet_error(ctx, "fswatch: init failed: %s", strerror(errno));
		return 1;
	}

	// Bind inotify fd to Skynet socket system
	is->socket_id = skynet_socket_bind(ctx, is->inotify_fd);
	if (is->socket_id < 0) {
		skynet_error(ctx, "fswatch: bind failed");
		close(is->inotify_fd);
		skynet_free(is);
		return 1;
	}

	// Start receiving events
	skynet_socket_start(ctx, is->socket_id);

	// Register callback
	skynet_callback(ctx, is, _inotify_cb);

	return 0;
}

// Release service
void
fswatch_release(struct fswatch_service *is) {
	// Remove all watches
	while (is->watches) {
		struct fswatch_watch *w = is->watches;
		remove_child_watches(is, w);
		is->watches = w->next;
		inotify_rm_watch(is->inotify_fd, w->wd);
		skynet_free(w->path);
		skynet_free(w);
	}

	if (is->socket_id >= 0) {
		skynet_socket_close(is->ctx, is->socket_id);
	}

	if (is->inotify_fd >= 0) {
		close(is->inotify_fd);
	}

	if (is->partial_buffer) {
		skynet_free(is->partial_buffer);
	}

	skynet_free(is);
}

// Optional signal handler
void
fswatch_signal(struct fswatch_service *is, int signal) {
	skynet_error(is->ctx, "fswatch: received signal %d", signal);
}

#else // !__linux__

// Stub implementation for non-Linux systems without inotify

struct fswatch_service {
	struct skynet_context *ctx;
};

struct fswatch_service *
fswatch_create(void) {
	struct fswatch_service *is = skynet_malloc(sizeof(*is));
	memset(is, 0, sizeof(*is));
	return is;
}

int
fswatch_init(struct fswatch_service *is, struct skynet_context *ctx, const char *parm) {
	is->ctx = ctx;
	skynet_error(ctx, "fswatch: file monitoring is not supported on this platform (requires Linux inotify)");
	// Still allow service creation, just log error
	return 0;
}

void
fswatch_release(struct fswatch_service *is) {
	skynet_free(is);
}

void
fswatch_signal(struct fswatch_service *is, int signal) {
	// Ignore
}

#endif // __linux__
