# ⚠️ Hokkaido Test Cluster Deployment Guide

> **WARNING:**  
> The scripts provided here are **specific to the Hokkaido Test Cluster**.  
> Do **not** use them in other environments without proper validation.

---

## 🧩 Prerequisites

Before deployment, ensure the following:

1. The **same NFS volume** is mounted on **I/O nodes 4–8**.  
2. The **`/mnt/raftdb` filesystem** is mounted on **I/O nodes 4–8**.

---

## 🚀 Deployment Steps

1. Copy all files to your Niova installation directory:
   ```bash
   cp ./* <NIOVA_DIR_PATH>
   ```

2. Run the deployment script with root privileges:
   ```bash
   sudo ./deploy.sh
   ```

---

## 💡 Notes

- The **CTL interface** can only be queried **from the node where the process is running**.  
  > This is because **inotify** does not function properly over NFS mounts.

---

## 🔍 Verification

To verify that both **server** and **client** processes are running:

```bash
pdsh -w 192.168.96.8[4-8] 'ps -ef | grep pumice-reference'
```

You should see, on each node:
- `pumice-reference-server`
- `pumice-reference-client`

---

## 🗂️ Logs and Configuration

All logs and configuration files will be available under:

```
/work/pumice
```
