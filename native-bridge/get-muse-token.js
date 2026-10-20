// get-muse-token.js — 在已登录的 muse.ai 页面里导出桥用的 token JSON
//
// 用途
//   桥（native_bridge.py）的凭证有两条路：cookies.txt + vm_id.txt（主力、
//   可自动续期）和 token.json（回退）。服务端把 cookies 失效时（auth/check
//   回 401 hatchAdmissionInvalidated、旧 token WSS 403），就靠这段脚本
//   重新导出一份 token JSON 装回去，桥即可恢复（2026-10-14 实操过一次）。
//
// 使用方法（详细版见同目录 get-muse-token.md）
//   【执行人：用户本人。靠的是用户自己的浏览器登录态，Muse
//   没有这个登录态、无法代跑；导出后把 JSON 以文件形式交给
//   Muse，由 Muse 装回桥并验证。】
//   1. 用浏览器打开 https://muse.ai 并确认已登录。
//   2. 按 F12 打开开发者工具，切到 Console（控制台）页签。
//   3. 把本文件整段粘贴进控制台，回车运行。
//   4. 控制台会打印一段 JSON 并自动复制到剪贴板，形如：
//        { "vm_id": "...", "access_token": "...", "hatch_token": "...",
//          "issued_at": "...", "issued_at_local": "...", "exp": null }
//   5. 把 JSON 存成文件交给 Muse，由 Muse 保存为
//      ~/.config/native-bridge/token.json 并设权限 0600；
//      若输出的 vm_id 与 ~/.config/native-bridge/vm_id.txt 里不同，
//      一并把 vm_id.txt 更新为新值。桥靠 cred_stamp 变化自动感知新凭证，
//      一般无需重启。
//
// 安全须知
//   输出里是等同登录凭证的真 token：不要贴进聊天、issue 或任何会同步/
//   入仓的地方，只存本机 0600 的 token.json；本脚本本身不含任何凭证，
//   可以正常入仓。
(async () => {
  const H = { 'Content-Type': 'application/json' };
  const j = async (r) => { if (!r.ok) throw new Error(r.status + ' ' + (await r.text()).slice(0, 200)); return r.json(); };
  try {
    const auth = await j(await fetch('/api/auth/check', { method: 'POST', headers: H, body: '', credentials: 'include' }));
    const access_token = auth.access_token;
    if (!access_token) throw new Error('auth/check 没返回 access_token，可能未登录');
    const sess = await j(await fetch('/api/session', { credentials: 'include' }));
    const vm_id = sess.vm_id;
    if (!vm_id) throw new Error('session 没返回 vm_id（VM 可能在重启）');
    const ht = await j(await fetch('/api/hatch/token', {
      method: 'POST',
      headers: { ...H, 'Authorization': 'Bearer ' + access_token },
      body: JSON.stringify({ vmAddress: `wss://${vm_id}.metaaivm.com/`, vmName: vm_id }),
      credentials: 'include'
    }));
    const hatch_token = ht.token;
    if (!hatch_token) throw new Error('hatch/token 没返回 token');
    const seg = hatch_token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
    const p = JSON.parse(atob(seg + '='.repeat((4 - seg.length % 4) % 4)));
    const out = {
      vm_id,
      access_token,
      hatch_token,
      issued_at: p.iat ? new Date(p.iat * 1000).toISOString() : null,
      issued_at_local: p.iat ? new Date(p.iat * 1000).toLocaleString() : null,
      exp: p.exp ?? null
    };
    const text = JSON.stringify(out, null, 2);
    console.log(text);
    try { copy(text); console.log('✅ 已复制到剪贴板'); } catch (e) {}
    return out;
  } catch (e) {
    console.error('❌ 出错了：', e.message);
  }
})();
