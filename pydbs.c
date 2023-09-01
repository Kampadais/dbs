// +build ignore

#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include "dbs.h"

static PyTypeObject device_info_type;
static PyTypeObject volume_info_type;
static PyTypeObject snapshot_info_type;

static PyStructSequence_Field device_info_fields[] = {
    {"version",                     NULL},
    {"device_size",                 NULL},
    {"total_device_extents",        NULL},
    {"allocated_device_extents",    NULL},
    {"volume_count",                NULL},
    {NULL}
};

static PyStructSequence_Field volume_info_fields[] = {
    {"volume_name",                 NULL},
    {"volume_size",                 NULL},
    {"snapshot_id",                 NULL},
    {"created_at",                  NULL},
    {"snapshot_count",              NULL},
    {NULL}
};

static PyStructSequence_Field snapshot_info_fields[] = {
    {"snapshot_id",                 NULL},
    {"parent_snapshot_id",          NULL},
    {"created_at",                  NULL},
    {NULL}
};

static PyStructSequence_Desc device_info_desc = {
    "pydbs.device_info",
    NULL,
    device_info_fields,
    5,
};

static PyStructSequence_Desc volume_info_desc = {
    "pydbs.volume_info",
    NULL,
    volume_info_fields,
    5,
};

static PyStructSequence_Desc snapshot_info_desc = {
    "pydbs.snapshot_info",
    NULL,
    snapshot_info_fields,
    3,
};

// Query API

static PyObject *get_device_info(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;

    static char *kwlist[] = {"device", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &device))
        return NULL;

    dbs_device_info device_info;
    dbs_bool ret = dbs_fill_device_info(device, &device_info);
    if (!ret)
        Py_RETURN_FALSE;

    PyObject *device_info_tuple = PyStructSequence_New(&device_info_type);
    if (device_info_tuple == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    PyStructSequence_SET_ITEM(device_info_tuple, 0, Py_BuildValue("k", device_info.version));
    PyStructSequence_SET_ITEM(device_info_tuple, 1, Py_BuildValue("K", device_info.device_size));
    PyStructSequence_SET_ITEM(device_info_tuple, 2, Py_BuildValue("k", device_info.total_device_extents));
    PyStructSequence_SET_ITEM(device_info_tuple, 3, Py_BuildValue("k", device_info.allocated_device_extents));
    PyStructSequence_SET_ITEM(device_info_tuple, 4, Py_BuildValue("B", device_info.volume_count));
    if (PyErr_Occurred()) {
        Py_DECREF(device_info_tuple);
        PyErr_NoMemory();
        return NULL;
    }
    return device_info_tuple;
}

static PyObject *get_volume_info(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;

    static char *kwlist[] = {"device", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &device))
        return NULL;

    dbs_volume_info volume_info[DBS_MAX_VOLUMES];
    uint8_t volume_count = 0;
    dbs_bool ret = dbs_fill_volume_info(device, volume_info, &volume_count);
    if (!ret)
        Py_RETURN_FALSE;

    PyObject *volume_info_list = PyList_New(volume_count);
    for (int i = 0; i < volume_count; i ++) {
        PyObject *volume_info_tuple = PyStructSequence_New(&volume_info_type);
        if (volume_info_tuple == NULL) {
            // Py_DECREF(volume_info_list);
            PyErr_NoMemory();
            return NULL;
        }
        PyStructSequence_SET_ITEM(volume_info_tuple, 0, Py_BuildValue("s", volume_info[i].volume_name));
        PyStructSequence_SET_ITEM(volume_info_tuple, 1, Py_BuildValue("K", volume_info[i].volume_size));
        PyStructSequence_SET_ITEM(volume_info_tuple, 2, Py_BuildValue("H", volume_info[i].snapshot_id));
        PyStructSequence_SET_ITEM(volume_info_tuple, 3, Py_BuildValue("L", volume_info[i].created_at));
        PyStructSequence_SET_ITEM(volume_info_tuple, 4, Py_BuildValue("H", volume_info[i].snapshot_count));
        if (PyErr_Occurred()) {
            Py_DECREF(volume_info_tuple);
            // Py_DECREF(volume_info_list);
            PyErr_NoMemory();
            return NULL;
        }

        PyList_SET_ITEM(volume_info_list, i, volume_info_tuple);
        // Py_DECREF(volume_info_tuple);
    }
    return volume_info_list;
}

static PyObject *get_snapshot_info(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;

    static char *kwlist[] = {"device", "volume_name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", kwlist, &device, &volume_name))
        return NULL;

    dbs_snapshot_info snapshot_info[DBS_MAX_SNAPSHOTS];
    uint16_t snapshot_count = 0;
    dbs_bool ret = dbs_fill_snapshot_info(device, volume_name, snapshot_info, &snapshot_count);
    if (!ret)
        Py_RETURN_FALSE;

    PyObject *snapshot_info_list = PyList_New(snapshot_count);
    for (int i = 0; i < snapshot_count; i ++) {
        PyObject *snapshot_info_tuple = PyStructSequence_New(&snapshot_info_type);
        if (snapshot_info_tuple == NULL) {
            // Py_DECREF(snapshot_info_list);
            PyErr_NoMemory();
            return NULL;
        }
        PyStructSequence_SET_ITEM(snapshot_info_tuple, 0, Py_BuildValue("H", snapshot_info[i].snapshot_id));
        if (snapshot_info[i].parent_snapshot_id) {
            PyStructSequence_SET_ITEM(snapshot_info_tuple, 1, Py_BuildValue("H", snapshot_info[i].parent_snapshot_id));
        } else {
            PyStructSequence_SET_ITEM(snapshot_info_tuple, 1, Py_BuildValue("O", Py_None));
        }
        PyStructSequence_SET_ITEM(snapshot_info_tuple, 2, Py_BuildValue("L", snapshot_info[i].created_at));
        if (PyErr_Occurred()) {
            Py_DECREF(snapshot_info_tuple);
            // Py_DECREF(snapshot_info_list);
            PyErr_NoMemory();
            return NULL;
        }

        PyList_SET_ITEM(snapshot_info_list, i, snapshot_info_tuple);
        // Py_DECREF(snapshot_info_tuple);
    }
    return snapshot_info_list;
}

// Management API

static PyObject *init_device(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;

    static char *kwlist[] = {"device", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &device))
        return NULL;

    dbs_bool ret = dbs_init_device(device);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *vacuum_device(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;

    static char *kwlist[] = {"device", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &device))
        return NULL;

    dbs_bool ret = dbs_vacuum_device(device);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *create_volume(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;
    uint64_t volume_size;

    static char *kwlist[] = {"device", "volume_name", "volume_size", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ssK", kwlist, &device, &volume_name, &volume_size))
        return NULL;

    dbs_bool ret = dbs_create_volume(device, volume_name, volume_size);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *rename_volume(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;
    char *new_volume_name;

    static char *kwlist[] = {"device", "volume_name", "new_volume_name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "sss", kwlist, &device, &volume_name, &new_volume_name))
        return NULL;

    dbs_bool ret = dbs_rename_volume(device, volume_name, new_volume_name);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *create_snapshot(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;

    static char *kwlist[] = {"device", "volume_name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", kwlist, &device, &volume_name))
        return NULL;

    dbs_bool ret = dbs_create_snapshot(device, volume_name);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *clone_snapshot(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;
    uint16_t snapshot_id;

    static char *kwlist[] = {"device", "volume_name", "snapshot_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ssH", kwlist, &device, &volume_name, &snapshot_id))
        return NULL;

    dbs_bool ret = dbs_clone_snapshot(device, volume_name, snapshot_id);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *delete_volume(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;

    static char *kwlist[] = {"device", "volume_name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", kwlist, &device, &volume_name))
        return NULL;

    dbs_bool ret = dbs_delete_volume(device, volume_name);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *delete_snapshot(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    uint16_t snapshot_id;

    static char *kwlist[] = {"device", "snapshot_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "sH", kwlist, &device, &snapshot_id))
        return NULL;

    dbs_bool ret = dbs_delete_snapshot(device, snapshot_id);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

// Block API

void close_volume(PyObject *capsule) {
    dbs_context context = (dbs_context)PyCapsule_GetPointer(capsule, NULL);
    if (context == NULL)
        return;

    dbs_close_volume(context);
}

static PyObject *open_volume(PyObject* self, PyObject *args, PyObject *kw) {
    char *device;
    char *volume_name;

    static char *kwlist[] = {"device", "volume_name", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "ss", kwlist, &device, &volume_name))
        return NULL;

    dbs_context context = dbs_open_volume(device, volume_name);
    if (context == NULL)
        return NULL;

    return PyCapsule_New((void *)context, NULL, close_volume);
}

static PyObject *read_block(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    uint64_t block;

    static char *kwlist[] = {"context", "block", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OK", kwlist, &capsule, &block))
        return NULL;

    dbs_context context = (dbs_context)PyCapsule_GetPointer(capsule, NULL);
    if (context == NULL)
        return NULL;

    char data[512];
    dbs_bool ret = dbs_read_block(context, block, (void *)data);
    if (!ret)
        return NULL;

    return Py_BuildValue("y#", data, 512);
}

static PyObject *write_block(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    uint64_t block;
    const char *data;
    size_t size;

    static char *kwlist[] = {"context", "block", "data", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OKy#", kwlist, &capsule, &block, &data, &size))
        return NULL;

    dbs_context context = (dbs_context)PyCapsule_GetPointer(capsule, NULL);
    if (context == NULL)
        return NULL;
    if (size < 512)
        return NULL;

    dbs_bool ret = dbs_write_block(context, block, (void *)data);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyObject *unmap_block(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    uint64_t block;

    static char *kwlist[] = {"context", "block", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OK", kwlist, &capsule, &block))
        return NULL;

    dbs_context context = (dbs_context)PyCapsule_GetPointer(capsule, NULL);
    if (context == NULL)
        return NULL;

    dbs_bool ret = dbs_unmap_block(context, block);
    return Py_BuildValue("O", ret ? Py_True : Py_False);
}

static PyMethodDef module_functions[] = {
    // Query API
    {"get_device_info",     (PyCFunction)get_device_info,       METH_VARARGS|METH_KEYWORDS, NULL},
    {"get_volume_info",     (PyCFunction)get_volume_info,       METH_VARARGS|METH_KEYWORDS, NULL},
    {"get_snapshot_info",   (PyCFunction)get_snapshot_info,     METH_VARARGS|METH_KEYWORDS, NULL},

    // Management API
    {"init_device",         (PyCFunction)init_device,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"vacuum_device",       (PyCFunction)vacuum_device,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_volume",       (PyCFunction)create_volume,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"rename_volume",       (PyCFunction)rename_volume,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_snapshot",     (PyCFunction)create_snapshot,       METH_VARARGS|METH_KEYWORDS, NULL},
    {"clone_snapshot",      (PyCFunction)clone_snapshot,        METH_VARARGS|METH_KEYWORDS, NULL},
    {"delete_volume",       (PyCFunction)delete_volume,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"delete_snapshot",     (PyCFunction)delete_snapshot,       METH_VARARGS|METH_KEYWORDS, NULL},

    // Block API
    {"open_volume",         (PyCFunction)open_volume,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"read_block",          (PyCFunction)read_block,            METH_VARARGS|METH_KEYWORDS, NULL},
    {"write_block",         (PyCFunction)write_block,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"unmap_block",         (PyCFunction)unmap_block,           METH_VARARGS|METH_KEYWORDS, NULL},

    {NULL}
};

static struct PyModuleDef module_definition = {
    PyModuleDef_HEAD_INIT,
    "pydbs",
    "Python interface to DBS",
    -1,
    module_functions
};

PyMODINIT_FUNC PyInit_pydbs(void) {
    PyObject *module = PyModule_Create(&module_definition);

    PyStructSequence_InitType(&device_info_type, &device_info_desc);
    PyStructSequence_InitType(&volume_info_type, &volume_info_desc);
    PyStructSequence_InitType(&snapshot_info_type, &snapshot_info_desc);
    Py_INCREF((PyObject *)&device_info_type);
    Py_INCREF((PyObject *)&volume_info_type);
    Py_INCREF((PyObject *)&snapshot_info_type);

    return module;
}
