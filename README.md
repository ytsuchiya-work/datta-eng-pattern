# datta-eng-pattern — データ取り込みパターン 30 通り比較アプリ

データ取り込みの段階で考えられる **取り込み方式 × 更新方法 × 更新タイミング** の組み合わせを、1 画面で比較できる Databricks App です。
各組み合わせについて「どう動くか（イメージ）」と「実際にどう動いたか（実測）」を、同じ時間軸に並べて見られます。

| 軸 | 選択肢 |
|---|---|
| 取り込み方式 | Auto Loader / COPY INTO |
| 更新方法 | 追記 / フルリフレッシュ / CDC |
| 更新タイミング | 手動バッチ / 定期スケジュール / ファイル到着トリガー / テーブル更新トリガー / Streaming |

2 × 3 × 5 = **30 通り**のうち、COPY INTO × Streaming の 5 通りは仕組み上できないため「不可」と表示します。残り 25 通りは、実際に Lakeflow Jobs を動かして確かめられます。

---

## 目次

1. [画面の構成](#画面の構成)
2. [検証パターン（30 通りの評価）](#検証パターン30-通りの評価)
3. [各方法の特徴と使いどころ](#各方法の特徴と使いどころ)
4. [組み合わせの選び方](#組み合わせの選び方)
5. [仕組み（アーキテクチャ）](#仕組みアーキテクチャ)
6. [実測結果](#実測結果)
7. [注意点・ハマりどころ](#注意点ハマりどころ)
8. [デプロイ手順](#デプロイ手順)
9. [ローカル開発](#ローカル開発)
10. [ディレクトリ構成](#ディレクトリ構成)

---

## 画面の構成

画面は 3 列で、左から「選ぶ」「見る」「動かす」の順に並んでいます。

| 場所 | 内容 |
|---|---|
| ヘッダー | ソースの変更件数（追加 / 更新 / 削除）、📤 ファイルを投入、⏹ 全停止、リセット |
| 左列 | 3 軸のセレクター、30 通りのマトリクス（◎ 定番 / ○ 可能 / △ 注意 / ✕ 不可、実行中のマーク、silver の一致率）、選んだ組み合わせの解説（仕組み、鮮度の目安、コスト感、注意点） |
| 中央列 | データの流れ図（ソースDB → ランディング → トリガー → 取り込み → bronze → 更新適用 → silver）、イメージのタイムライン（再生・スクラブ可）、実測のタイムライン（10 / 15 / 30 / 60 分）、ファイルごとの遅延表 |
| 右列 | 操作パネル（タイミングの適用、今すぐ実行、bronze へ取り込み（上流）、停止、自動停止までの残り時間、ジョブへのリンク）とデータタブ（silver とソースの突き合わせ / bronze / ランディング / Delta 履歴 / 実際のコードとトリガー設定 JSON） |

### イメージと実測

- **イメージ**: ファイルが 150 秒の間に 7 個届くという想定で、各タイミングなら「いつ起動し、いつ bronze と silver に反映されるか」をアニメーションで示します。
- **実測**: 実際のジョブ実行（起動待ちも含む）、bronze と silver の Delta コミット、ファイルが着地した時刻を同じ時間軸に並べます。ファイルごとに「着地 → bronze → silver」が何秒かかったかも表示します。

### 基本の使い方

1. 左のマトリクスで組み合わせを選びます。
2. 右の操作パネルでタイミングを「適用」します。
3. ヘッダーの「📤 ファイルを投入」で、ソースに追加・更新・削除を起こします。
4. 中央のタイムラインと右の「silver 突き合わせ」で、いつ・どこまで反映されたかを確認します。

タイミングごとの手順は、操作パネルの「手順」にも表示されます。

| タイミング | 手順 |
|---|---|
| 手動バッチ | ファイルを投入 → ▶ 今すぐ実行 |
| 定期スケジュール | 適用（2 分ごとに起動）→ ファイルを投入 → 次の起動で反映 |
| ファイル到着 | 適用 → ファイルを投入 → 新しいファイルを検知してジョブが自動起動 |
| テーブル更新 | 適用 → ファイルを投入 → 「bronze へ取り込み（上流）」→ bronze の更新を検知して silver 反映ジョブが自動起動 |
| Streaming | 適用（ストリームが起動、最大 10 分）→ ファイルを投入 → 数十秒で silver に反映 |

---

## 検証パターン（30 通りの評価）

◎ 定番 / ○ 可能 / △ 注意 / ✕ 不可

| 取り込み | 更新方法 | 手動 | 定期 | ファイル到着 | テーブル更新 | Streaming |
|---|---|:-:|:-:|:-:|:-:|:-:|
| Auto Loader | 追記 | ○ | ◎ | ◎ | ○ | ◎ |
| Auto Loader | フルリフレッシュ | △ | △ | △ | △ | △ |
| Auto Loader | CDC | ○ | ◎ | ◎ | ○ | ◎ |
| COPY INTO | 追記 | ◎ | ◎ | ○ | ○ | ✕ |
| COPY INTO | フルリフレッシュ | ○ | ◎ | ○ | ○ | ✕ |
| COPY INTO | CDC | ○ | ◎ | ○ | ○ | ✕ |

評価の理由:

- **Auto Loader × フルリフレッシュが △**: Auto Loader は「新しいファイルだけを読む」増分処理のための仕組みです。毎回全件を置き換えるなら、`read_files` ＋ `CREATE OR REPLACE TABLE`、マテリアライズドビュー、宣言型パイプラインの `AUTO CDC FROM SNAPSHOT` のほうが素直です。Streaming と組み合わせると、マイクロバッチのたびに全件を上書きするため重くなります。
- **COPY INTO × Streaming が ✕**: COPY INTO はバッチ専用の SQL コマンドで、常時起動はできません。
- **Auto Loader × 定期 / ファイル到着が ◎**: `availableNow` で未処理のファイルだけを処理して終了するため、常時起動せずに増分処理できる定番の構成です。
- **COPY INTO × ファイル到着が ○**: 動作はします。ただ、ファイル数が多い場合や到着が頻繁な場合は Auto Loader のほうがスケールします。

### 6 つの取り込みパターン（テーブル）

1 回の「ファイルを投入」で、6 パターンそれぞれのランディングに、そのパターン用のファイルが同時に着地します。

| パターン ID | 取り込み | 更新方法 | ランディングに置くファイル | bronze → silver の処理 |
|---|---|---|---|---|
| `al_append` / `ci_append` | AL / CI | 追記 | 新規レコードのみ | `INSERT`（silver に未反映のファイル分） |
| `al_full` / `ci_full` | AL / CI | フルリフレッシュ | 全件スナップショット | `INSERT OVERWRITE`（最新スナップショット） |
| `al_cdc` / `ci_cdc` | AL / CI | CDC | 変更レコード（`_op` = INSERT / UPDATE / DELETE、順序キー `_seq`） | `MERGE`（キーごとに最新の変更、`_seq` が新しいときだけ反映） |

---

## 各方法の特徴と使いどころ

### 取り込み方式

| | Auto Loader | COPY INTO |
|---|---|---|
| 正体 | `cloudFiles` ソースの Structured Streaming | SQL コマンド |
| 処理済みファイルの管理 | チェックポイント | 取り込み先のテーブル側が記録（再実行しても二重取り込みしない） |
| 実行場所 | ノートブック / 宣言型パイプライン（このアプリはサーバーレスのノートブック） | SQL Warehouse / ノートブック（このアプリは SQL Warehouse の SQL タスク） |
| 起動方法 | `availableNow`（バッチ的に実行して終了）または常時起動 | バッチのみ |
| スキーマ | 推論とスキーマ進化に対応 | 推論も可能（このアプリでは列を明示） |
| このアプリでの所要時間 | 約 55 秒（サーバーレスの起動込み） | 約 10 秒 |
| 向いている場面 | ファイルが大量にある、継続的に届く、秒〜分単位の鮮度が必要 | ファイル数がそれほど多くない、SQL だけで手軽に済ませたい、アドホックな再取り込み |

公式ドキュメントでは、ファイル数が数千程度までなら COPY INTO、時間とともに数百万以上に増えるなら Auto Loader が目安とされています。

### 更新方法

| | 追記 | フルリフレッシュ | CDC |
|---|---|---|---|
| ソースが出すもの | 新規レコード | 全件スナップショット | 変更レコード（操作種別＋順序キー） |
| silver への処理 | `INSERT` | `INSERT OVERWRITE` | `MERGE`（重複排除＋順序の確認） |
| 更新・削除の反映 | **されない** | される（毎回まるごと置き換え） | される（差分だけ） |
| 1 回あたりのコスト | 最小 | データ全体の量に比例 | 変更量に比例 |
| 向いているデータ | ログ、イベント、明細など追記しかないもの | 小さなマスタ、差分を取れないソース、毎回の完全一致が必要なもの | 大きな業務テーブル、更新・削除があり差分を取れるソース（DB の CDC、変更ログ） |
| 気をつける点 | 更新や削除が起きるデータに使うと、silver が古いまま残る | データが増えるとコストと時間が増え続ける | 順序キーによる並べ替えと重複排除が必須。宣言型パイプラインなら `AUTO CDC` で SCD Type 1 / 2 を宣言的に書ける |

アプリの「silver 突き合わせ」タブでは、ソースと silver の各行を「一致 / 古い / 未反映 / ソースで削除済」に分類します。追記パターンだけ「古い」「ソースで削除済」が残ることで、違いが目で見てわかります。

### 更新タイミング

| | 手動バッチ | 定期スケジュール | ファイル到着トリガー | テーブル更新トリガー | Streaming |
|---|---|---|---|---|---|
| 起動のきっかけ | 人や外部システムが run-now | cron（このアプリでは 2 分ごと） | 監視パスへの新しいファイルの到着 | 上流テーブルの更新 | 常時起動 |
| 鮮度の目安 | 実行するまで反映されない | 最大で間隔＋処理時間 | 検知まで約 1〜1.5 分＋処理時間 | 上流の検知＋silver 反映（約 10 秒） | 数秒〜数十秒 |
| コスト | 実行したときだけ | データがなくても間隔ごとに起動 | ファイルが届いたときだけ | 上流が更新されたときだけ | 起動中はずっとかかる |
| 向いている場面 | 初回ロード、再処理、外部オーケストレーターからの起動 | 「毎時」「毎朝」など業務の締めに合わせた更新 | ファイルが不定期に届く、来たらすぐ処理したいが常時起動は避けたい | 多段パイプラインの 2 段目以降（bronze → silver → gold） | 秒〜数十秒の鮮度が必要、ファイルが絶えず届く |
| このアプリでの実装 | `run-now` | `schedule`（Quartz cron `0 0/2 * * * ?`） | `trigger.file_arrival`（監視先 `landing/<パターン>/`、最小間隔 60 秒） | silver 反映専用ジョブの `trigger.table_update`（bronze を監視） | Auto Loader を常時起動（10 秒間隔、最大 10 分で自動停止）し、マイクロバッチごとに silver まで反映 |

---

## 組み合わせの選び方

| やりたいこと | おすすめの組み合わせ |
|---|---|
| ログやイベントを、なるべく早く安く取り込みたい | Auto Loader × 追記 × ファイル到着（または定期） |
| 秒単位の鮮度が必要 | Auto Loader × 追記 / CDC × Streaming |
| 業務 DB の更新・削除まで、差分で反映したい | Auto Loader または COPY INTO × CDC × 定期 / ファイル到着 |
| 小さなマスタを毎日まるごと入れ替えたい | COPY INTO × フルリフレッシュ × 定期 |
| SQL だけで手軽に始めたい | COPY INTO × 追記 × 手動 / 定期 |
| bronze → silver → gold を連鎖させたい | 1 段目は上のどれか、2 段目以降はテーブル更新トリガー |
| 本番運用で CDC や SCD2 を楽にしたい | Lakeflow 宣言型パイプライン（Auto Loader ＋ `AUTO CDC`）を検討する |

---

## 仕組み（アーキテクチャ）

```
┌──────────── Databricks App（FastAPI ＋ React、1 プロセス） ────────────┐
│  ・UI 配信と REST API                                                    │
│  ・起動時の bootstrap（テーブル・Volume・SQL・ノートブック・ジョブ作成）  │
│  ・疑似ソース DB の変更 → 6 パターン分のファイルをランディングへ出力      │
│  ・自動停止の監視（30 分）、ファイル到着の代行監視（後述）               │
└───────┬───────────────────────────────┬──────────────────────────────┘
        │ Statement Execution API        │ Jobs API / Files API
        ▼                                ▼
  SQL Warehouse                   Lakeflow Jobs（12 本、App のサービスプリンシパルで実行）
                                    [data-eng-pattern] <パターン>
                                      ingest_bronze → check_stage → apply_silver
                                    [data-eng-pattern] <パターン> silver-on-table-update
                                      apply_silver（テーブル更新トリガー用）
        ▼
  Unity Catalog: <CATALOG>.<SCHEMA>
    source_orders（疑似ソース）、event_log
    {al,ci}_{append,full,cdc}_bronze、{al,ci}_{append,full,cdc}（silver）
    Volume: landing/<パターン>/（ファイルの着地点）、checkpoints/（Auto Loader 用）
```

- **ingest_bronze**: Auto Loader はサーバーレスのノートブックタスク（`app/assets/autoloader_ingest.txt`）、COPY INTO は SQL タスクです。
- **check_stage**: condition task です。ジョブパラメータ `stage` が `all` のときだけ、次の silver 反映に進みます。テーブル更新トリガーのデモでは `stage=bronze` で上流だけを動かします。
- **apply_silver**: 更新方法ごとの SQL（`INSERT` / `INSERT OVERWRITE` / `MERGE`）を実行します。
- **Streaming**: 同じジョブを `mode=stream` で起動します。ノートブックの `foreachBatch` が、bronze への追記と silver への反映をマイクロバッチごとに行います。
- **タイミングの切り替え**: 「適用」すると、いったん全トリガーを外してから、選んだ 1 つだけを `jobs/update` で有効にします。適用中のタイミングはジョブのタグ（`timing` / `enabled_at`）に記録します。
- **疑似ソース**: `source_orders` は受注データで、ステータスが「受注 → 出荷準備 → 出荷済 → 配達完了」と進みます。「ファイルを投入」で追加・更新・削除を起こし、同じ名前の JSONL ファイルを 6 パターンのランディングへ同時に書き出します。

各パターンで実際に動く SQL、ノートブック、トリガー設定 JSON は、アプリの「コード」タブで確認できます。

---

## 実測結果

本番のワークスペースで、App のサービスプリンシパルが作ったジョブを使って計測しました。

| 項目 | 結果 |
|---|---|
| 手動バッチ（6 パターン） | すべて成功。Auto Loader は約 55 秒（サーバーレスの起動込み）、COPY INTO は約 10 秒 |
| 定期スケジュール（2 分ごと） | スケジュールどおり起動（`PERIODIC`） |
| テーブル更新トリガー | bronze の更新を検知して silver 反映ジョブが起動（`TABLE`） |
| Streaming（Auto Loader） | ファイル着地から silver 反映まで約 20〜26 秒 |
| ファイル到着トリガー | ジョブ側のトリガーは起動せず、アプリの代行監視で約 90 秒後に起動し、ソースと 100% 一致 |
| 更新方法の違い | 追記では更新・削除が反映されず、フルリフレッシュと CDC ではソースと一致 |

所要時間はワークスペース、リージョン、サーバーレスの起動状況によって変わります。

---

## 注意点・ハマりどころ

### ファイル到着トリガーと代行監視

- 検証したワークスペースでは、**ジョブのファイル到着トリガーが起動しませんでした**。managed / external の両方の Volume、作成時の設定 / 後からの設定、ユーザー所有 / サービスプリンシパル所有のジョブのどれで試しても、10 分待って起動しませんでした。managed Volume では `trigger_state.using_file_events: true` にはなりましたが、それでも起動しませんでした。
- そこでアプリが landing を 15 秒ごとに確認し、**新しいファイルが来てから 75 秒以内にジョブが起動しなければ、アプリが run-now します**（`FA_GRACE_SEC` で変更可）。このときタイムラインには「ファイル到着（アプリ代行）」と表示します。
- ジョブ側のトリガーも設定したままなので、トリガーが正しく動く環境では、そちらが先に起動します。
- `trigger.file_arrival.url` は末尾が `/` でないと拒否されます（`must end with '/'`）。

### コストと安全装置

- スケジュール、ファイル到着、テーブル更新のトリガーは、適用から **30 分で自動的に手動へ戻ります**（`AUTO_OFF_MIN`）。
- Streaming は最大 **10 分**で停止します（`STREAM_MIN`）。
- ヘッダーの「⏹ 全停止」で、全トリガーの解除と実行中ジョブの停止をすぐに行えます。
- 定期スケジュールは、データがなくても毎回起動するため、その都度コストがかかります。

### 方式ごとの注意

- **サーバーレスの Streaming**: サーバーレスでは `processingTime` トリガーが使えません。このアプリでは代わりに、`availableNow` を 10 秒間隔で繰り返して常時処理しています。常時起動を本番で行うなら、宣言型パイプラインの連続モードやクラシックコンピュートも検討してください。
- **CDC**: 同じキーへの変更が 1 回の処理に複数含まれることがあります。`_seq` で並べてキーごとに最新の 1 件に絞り、さらに「`_seq` が silver より新しいときだけ反映する」条件を付けて、再実行しても壊れない（冪等な）処理にしています。
- **追記**: silver に未反映のファイル（`_file`）の行だけを追加するので、再実行しても重複しません。ただし、ソース側の更新・削除は反映されません。
- **フルリフレッシュ**: bronze に溜まったスナップショットのうち最新の 1 ファイルで、silver を置き換えます。bronze は追記で増え続けるため、本番では保持期間の管理が必要です。
- **同時実行**: 各ジョブは `max_concurrent_runs: 1`、キュー有効にしてあります。同時に起動されると後のものが待ちになるため、タイムラインに起動待ちの時間として表れます。

### アプリの運用

- アプリを再起動・再デプロイすると、起動時の bootstrap がジョブ設定を `jobs/reset` で初期状態に戻すため、**適用中のタイミングは手動に戻ります**。
- 「リセット」は、6 パターンのテーブル、ランディング、チェックポイント、イベントログを初期化し、ソースを 12 行で作り直します。

### 実装でハマったところ

- `databricks workspace import-dir` は、先頭に `# Databricks notebook source` がある `.py` をノートブックに変換します。App のソースに置くとファイルとして読めず、bootstrap が止まります。そのため、ノートブックの元は `.txt`（`app/assets/autoloader_ingest.txt`）で置き、bootstrap 時にノートブックとして import しています。
- Jobs API `jobs/list` の `name` は完全一致のフィルタです。タグで絞りたいときは、ページングしながら自前でフィルタします。
- Statement Execution API は、値をすべて文字列で返します。列の `type_name` を見て数値や真偽値に変換しています。

---

## デプロイ手順

### 前提

- Unity Catalog とサーバーレス（ノートブック / ジョブ）が使えるワークスペース
- SQL Warehouse（サーバーレス推奨）
- Databricks CLI v0.200 以降、Node.js 18 以降

### 1. 設定

`app/app.yaml` の環境変数を、自分の環境に合わせて設定します。

| 変数 | 内容 |
|---|---|
| `CATALOG` / `SCHEMA` | データを溜めるカタログとスキーマ（既定: `classic_stable_ytcy_catalog.data_eng_pattern`） |
| `OWNER_USER` | 作成したジョブとワークスペースフォルダに CAN_MANAGE を付けるユーザー（自分のメールアドレス）。空なら付けない |
| `AUTO_OFF_MIN` | トリガーを自動で手動へ戻すまでの分数（既定 30） |
| `STREAM_MIN` | Streaming の最大起動時間（分、既定 10） |
| `WAREHOUSE_ID` | `valueFrom: sql-warehouse`（App のリソースから自動で入る） |

### 2. フロントエンドのビルド

```bash
cd client
npm ci
npm run build   # → app/static に出力
```

### 3. App の作成と権限

```bash
PROFILE=<your-profile>
APP=data-eng-pattern

databricks apps create $APP -p $PROFILE
```

App の「リソース」に SQL Warehouse を追加します（キー `sql-warehouse`、権限 `CAN_USE`）。UI からでも追加できます。

次に、App のサービスプリンシパルに Unity Catalog の権限を付けます。サービスプリンシパルの ID は `databricks apps get $APP` の `service_principal_client_id` で確認できます。

```sql
CREATE SCHEMA IF NOT EXISTS <CATALOG>.<SCHEMA>;
GRANT USE CATALOG ON CATALOG <CATALOG> TO `<service_principal_client_id>`;
GRANT ALL PRIVILEGES ON SCHEMA <CATALOG>.<SCHEMA> TO `<service_principal_client_id>`;
```

### 4. デプロイ

```bash
WS_PATH=/Workspace/Users/<you>/data-eng-pattern-app
databricks workspace import-dir app $WS_PATH --overwrite -p $PROFILE
databricks apps deploy $APP --source-code-path $WS_PATH -p $PROFILE
```

App が起動すると、サービスプリンシパルが bootstrap を行い、次のものを作ります。完了までの間、画面上部に「初期化中」と表示されます。

- Volume（landing / checkpoints）とテーブル 14 個
- サービスプリンシパルのホーム配下の SQL ファイルとノートブック
- ジョブ 12 本
- 初期データ（12 行）

---

## ローカル開発

```bash
# バックエンド（ポートは DATABRICKS_APP_PORT、既定 8000）
python -m venv .venv && source .venv/bin/activate
pip install fastapi uvicorn "databricks-sdk>=0.60"
cd app
DATABRICKS_CONFIG_PROFILE=<your-profile> WAREHOUSE_ID=<warehouse-id> \
  SCHEMA=data_eng_pattern_dev python server.py

# フロントエンド（別ターミナル。/api を localhost:8000 にプロキシ）
cd client && npm ci && npm run dev
```

ローカルで起動すると、自分の ID で bootstrap が走り、ジョブやテーブルが作られます。本番と混ざらないよう、`SCHEMA` と `APP_TAG`（ジョブ名とタグの接頭辞、既定 `data-eng-pattern`）を変えてください。

---

## ディレクトリ構成

```
.
├── app/                         # Databricks App 本体（デプロイ対象）
│   ├── app.yaml                 # 起動コマンドと環境変数
│   ├── requirements.txt
│   ├── server.py                # FastAPI（API と SPA 配信、バックグラウンドスレッドの起動）
│   ├── core.py                  # bootstrap、ジョブ管理、SQL 生成、疑似ソース、状態取得
│   ├── assets/
│   │   └── autoloader_ingest.txt  # Auto Loader ノートブックの元（batch / stream）
│   └── static/                  # フロントエンドのビルド結果（git 管理外）
└── client/                      # React ＋ Vite ＋ TypeScript
    └── src/
        ├── App.tsx              # 画面全体（3 列レイアウト）
        ├── knowledge.ts         # 3 軸の解説、30 通りの評価、注意点
        ├── concept.ts           # イメージのタイムライン（想定シナリオ）
        ├── Diagram.tsx          # データの流れ図（SVG）
        ├── Timelines.tsx        # イメージ / 実測のタイムライン
        ├── api.ts               # API クライアント
        └── styles.css
```

## ライセンス

[MIT](LICENSE)
