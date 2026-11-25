import { NexusNode, WebRtcTransport } from "../src/index.js";

const $ = (id) => document.getElementById(id);
let transport;
let node;

function log(message, details) {
  const suffix = details === undefined ? "" : ` ${JSON.stringify(details)}`;
  $("log").textContent = `${new Date().toLocaleTimeString()} ${message}${suffix}\n${$("log").textContent}`;
}

function peerId() {
  const id = $("remote-id").value.trim();
  if (!id) throw new Error("相手タブのIDを入力してください");
  return id;
}

function setStatus(text) {
  $("status").textContent = text;
}

function updatePeerStatus() {
  if (!transport) return;
  const peers = transport.getPeers();
  $("peer-status").textContent = peers.length
    ? JSON.stringify(peers)
    : "ピア未接続";
}

async function run(action) {
  try {
    await action();
  } catch (error) {
    setStatus(`エラー: ${error.message}`);
    log("ERROR", error.message);
  } finally {
    updatePeerStatus();
  }
}

$("initialize").addEventListener("click", () => run(() => {
  if (node) node.close();
  if (transport) transport.close();
  const rtcConfiguration = JSON.parse($("rtc-config").value || "{}");
  transport = new WebRtcTransport({ rtcConfiguration });
  node = new NexusNode(transport, { nodeId: $("local-id").value.trim() || undefined });
  node.subscribe((packet) => log("パケット受信", {
    objectId: packet.objectId,
    sequenceNumber: packet.sequenceNumber
  }));
  setStatus(`初期化済み: ${node.nodeId}`);
  log("NEXUSノードを初期化しました", { nodeId: node.nodeId });
}));

$("create-offer").addEventListener("click", () => run(async () => {
  if (!transport) throw new Error("先にこのタブを初期化してください");
  const id = peerId();
  node.addPeer(id);
  $("offer").value = await transport.createOffer(id);
  setStatus("Offer作成済み。相手タブで受け入れてください");
  log("Offerを作成しました", { peerId: id });
}));

$("accept-offer").addEventListener("click", () => run(async () => {
  if (!transport) throw new Error("先にこのタブを初期化してください");
  const id = peerId();
  node.addPeer(id);
  $("answer").value = await transport.acceptOffer(id, $("offer").value.trim());
  setStatus("Answer作成済み。Offer側へコピーしてください");
  log("Offerを受け入れ、Answerを作成しました", { peerId: id });
}));

$("apply-answer").addEventListener("click", () => run(async () => {
  if (!transport) throw new Error("先にこのタブを初期化してください");
  await transport.acceptAnswer(peerId(), $("answer").value.trim());
  setStatus("Answerを適用しました。ICE接続を待っています");
  log("Answerを適用しました", { peerId: peerId() });
}));

$("publish").addEventListener("click", () => run(async () => {
  if (!node) throw new Error("先にこのタブを初期化してください");
  const dataShards = Number($("data-shards").value);
  const parityShards = Number($("parity-shards").value);
  if (!Number.isInteger(dataShards) || dataShards < 1
    || !Number.isInteger(parityShards) || parityShards < 0
    || dataShards + parityShards > 255) {
    throw new RangeError("片数は data >= 1、parity >= 0、合計 <= 255 にしてください");
  }
  const result = await node.publish(new TextEncoder().encode($("payload").value), {
    dataShards,
    parityShards,
    expiresInMs: 60 * 60 * 1000
  });
  $("object-id").value = result.objectId;
  $("result").textContent = `${result.packetsCreated}片を配布しました: ${result.objectId}`;
  log("データを符号化して配布しました", result);
}));

$("retrieve").addEventListener("click", () => run(async () => {
  if (!node) throw new Error("先にこのタブを初期化してください");
  const objectId = $("object-id").value.trim();
  if (!objectId) throw new Error("Object IDを入力してください");
  const result = await node.retrieve(objectId);
  if (result.status === "recovered") {
    const text = new TextDecoder().decode(result.data);
    $("result").textContent = `復元成功 (${result.packetsFound}/${result.packetsNeeded}片): ${text}`;
    log("オブジェクトを復元しました", {
      objectId,
      packetsFound: result.packetsFound,
      packetsNeeded: result.packetsNeeded
    });
  } else {
    $("result").textContent =
      `${result.status} (${result.packetsFound}/${result.packetsNeeded ?? "不明"}片)`;
    log("回収できませんでした", result);
  }
}));

$("sync").addEventListener("click", () => run(async () => {
  if (!node) throw new Error("先にこのタブを初期化してください");
  const result = await node.syncPeer(peerId());
  log("保存済みパケットを相手へ同期しました", result);
}));

$("refresh").addEventListener("click", () => {
  updatePeerStatus();
  if (node) log("ノード状態", node.getStatus());
});

setInterval(updatePeerStatus, 1000);
