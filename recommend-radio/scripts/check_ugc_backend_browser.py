"""Real UGC API + temporary database + built frontend, without upstream writes."""
import json
import os
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'recommend-radio/backend'), str(ROOT/'src')]


def main():
    temporary = Path(tempfile.mkdtemp(prefix='ugc-http-browser-'))
    os.environ.update(APP_DATA_DIR=str(temporary/'db'), DATABASE_URL='', BUSINESS_DATABASE_URL='', AUTH_MODE='disabled', AMEM_ENABLED='false',
        AMEM_TRANSPORT='embedded', JOB_TRANSPORT='disabled', RABBITMQ_ENABLED='false',
        RECOMMEND_LLM_ENABLED='false', RECOMMEND_AUTO_DISCOVERY_ENABLED='false', AUTO_DREAM_ENABLED='false',
        UGC_ENABLED='true', CONTENT_SEARCH_ENABLED='false', FEED_ENABLED='false', REQUIRE_DB_MIGRATION='0')
    from database import init_db, get_connection, mysql_target, DEFAULT_DB_PATH
    assert mysql_target() is None and DEFAULT_DB_PATH.resolve().is_relative_to(temporary.resolve())
    init_db()
    with get_connection() as conn:
        for identity in ('ugc-owner','ugc-viewer'):
            conn.execute('INSERT INTO app_users(id,display_name,created_at,updated_at) VALUES (?,?,?,?)',(identity,identity,'now','now'))
    from app import oidc_auth, app as flask_app
    capabilities_fixture = not any(rule.rule == '/api/content/capabilities' for rule in flask_app.url_map.iter_rules())
    if capabilities_fixture:
        flask_app.add_url_rule('/api/content/capabilities', 'ugc_acceptance_capabilities',
            lambda: {'success':True,'data':{'ugcEnabled':True,'contentSearchEnabled':False}})
    original = oidc_auth.current_user
    oidc_auth.current_user = lambda raw_token,touch=True: oidc_auth.identity_service.get_user(raw_token) if raw_token in ('ugc-owner','ugc-viewer') else original(raw_token,touch=touch)
    import uvicorn
    from web_asgi import app
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=port,log_level='warning',access_log=False))
    thread = threading.Thread(target=server.run,daemon=True);thread.start()
    base=f'http://127.0.0.1:{port}'
    import requests
    for _ in range(100):
        try:
            if requests.get(base+'/health/live',timeout=.5).status_code==200:break
        except requests.RequestException:pass
        time.sleep(.1)
    report={'capabilitiesFixture':capabilities_fixture}
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as runner:
            browser=runner.chromium.launch();owner=browser.new_context();viewer=browser.new_context()
            owner.add_cookies([{'name':oidc_auth.cookie_name,'value':'ugc-owner','url':base}]);viewer.add_cookies([{'name':oidc_auth.cookie_name,'value':'ugc-viewer','url':base}])
            page=owner.new_page();other=viewer.new_page();errors=[];failures=[]
            page.on('pageerror',lambda error:errors.append(str(error)));other.on('pageerror',lambda error:errors.append(str(error)))
            page.on('response',lambda response:failures.append(response.url) if response.status>=500 else None)
            page.goto(base+'/#/studio')
            try: page.get_by_label('标题',exact=True).fill('真实 API 音乐创作',timeout=10000)
            except Exception:
                print({'body':page.locator('body').inner_text(),'errors':errors,'http500Count':len(failures)})
                raise
            page.get_by_label('正文',exact=True).fill('一段音乐创作记录，验证草稿、发布与权限。')
            page.get_by_label('可见范围',exact=True).select_option('public')
            page.get_by_role('button',name='保存',exact=True).click();page.get_by_text('已保存',exact=True).wait_for()
            with get_connection() as conn:
                row=conn.execute("SELECT * FROM ugc_posts WHERE author_id='ugc-owner'").fetchone();content_id=row['content_id'];version=row['version']
                assert row['status']=='draft'
            report['real_draft_transaction']=True
            other.goto(base+'/#/content/'+content_id);other.get_by_text('内容不存在，或当前账号没有查看权限。').wait_for()
            report['other_user_cannot_read_draft']=True
            page.get_by_role('button',name='发布',exact=True).click();page.get_by_text('已发布',exact=True).wait_for()
            other.reload();other.get_by_role('heading',name='真实 API 音乐创作').wait_for()
            assert other.locator('video').count()==0
            other.get_by_role('button',name='分享',exact=True).click();other.get_by_role('textbox',name='分享链接').wait_for()
            assert '/#/content/'+content_id in other.get_by_role('textbox',name='分享链接').input_value()
            report['published_plain_text_and_working_share_route']=True
            page.get_by_label('可见范围',exact=True).select_option('private')
            page.get_by_role('button',name='保存',exact=True).click();page.get_by_text('已保存',exact=True).wait_for()
            other.reload();other.get_by_text('内容不存在，或当前账号没有查看权限。').wait_for()
            with get_connection() as conn:
                row=conn.execute('SELECT * FROM ugc_posts WHERE content_id=?',(content_id,)).fetchone()
                assert row['version']>version and row['permission_version']>=3
            report['real_private_revision_revokes_other_viewer']=True
            page.get_by_role('link',name='查看详情',exact=True).click();page.get_by_role('heading',name='真实 API 音乐创作').wait_for()
            assert page.get_by_role('button',name='分享',exact=True).count()==0
            report['owner_private_detail_keeps_edit_without_public_share']=True
            page.get_by_role('link',name='编辑内容',exact=True).click()
            page.get_by_role('button',name='删除内容',exact=True).click();page.get_by_role('button',name='确认删除',exact=True).click();page.get_by_text('已删除',exact=True).wait_for()
            with get_connection() as conn:
                assert conn.execute('SELECT status FROM ugc_posts WHERE content_id=?',(content_id,)).fetchone()[0]=='deleted'
                events=conn.execute("SELECT COUNT(*) FROM domain_event_outbox WHERE json_extract(event_json,'$.aggregateId')=?",(content_id,)).fetchone()[0]
                assert events>=4
            report['delete_and_domain_outbox_committed']=True
            report['domainEvents']=events;report['javascriptErrors']=errors;report['http500Count']=len(failures)
            assert not errors and not failures
            browser.close()
    finally:
        server.should_exit=True;thread.join(timeout=10)
        (ROOT/'doc/ugc-real-api-browser-2026-10-08.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
