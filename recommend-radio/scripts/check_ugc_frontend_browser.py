"""UGC authoring/search/privacy browser acceptance with controlled API responses."""
import copy
import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def main():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    directory = Path(tempfile.mkdtemp(prefix='ugc-browser-'))
    log = (directory/'vite.log').open('w')
    server = subprocess.Popen(['node','node_modules/vite/bin/vite.js','--host','127.0.0.1','--port',str(port)],cwd=ROOT/'recommend-radio/frontend',stdout=log,stderr=log,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    posts, commands, writes, errors, checks = {}, {}, [], [], {}
    control = {'fail_create':True,'conflict':False,'delay_search':False,'delayed':None,'delay_suggest':False,'delayed_suggest':None}
    base = f'http://127.0.0.1:{port}/tests/content-browser.html'
    def material(post, owner):
        result = copy.deepcopy(post); result['canEdit'] = owner; result['source'] = 'ugc'
        return result
    try:
        with sync_playwright() as runner:
            browser = runner.chromium.launch()
            owner = browser.new_page(viewport={'width':1200,'height':900})
            viewer = browser.new_page(viewport={'width':390,'height':844})
            for page in (owner, viewer): page.on('pageerror', lambda error:errors.append(str(error)))
            def api(route, is_owner):
                parsed = urlparse(route.request.url); path = parsed.path
                if not path.startswith('/api/'):
                    route.continue_(); return
                def reply(data=None, status=200): route.fulfill(status=status,content_type='application/json',body=json.dumps({'success':status<400,'data':data,'error':{'code':'fixture','message':'Content changed' if status==409 else 'Unavailable'}}))
                if path == '/api/content/capabilities': reply({'ugcEnabled':True,'contentSearchEnabled':True})
                elif path == '/api/ugc/media-candidates': reply({'items':[]})
                elif path in ('/api/content/search','/api/content/suggest'):
                    query = parse_qs(parsed.query).get('q',[''])[0]
                    rows = [material(post,is_owner) for post in posts.values() if post['status']=='published' and (post['visibility']=='public' or is_owner) and query.lower() in post['title'].lower()]
                    if path.endswith('/search') and control['delay_search']:
                        control['delay_search']=False;control['delayed']=(route,rows,query);return
                    if path.endswith('/suggest') and control['delay_suggest']:
                        control['delay_suggest']=False;control['delayed_suggest']=(route,rows);return
                    reply({'items':rows,'query':query,'hasMore':False,'nextCursor':None})
                elif path == '/api/ugc/posts' and route.request.method=='GET': reply({'items':[material(post,True) for post in posts.values() if post['status']!='deleted']})
                elif path.startswith('/api/ugc/posts'):
                    method=route.request.method;parts=path.split('/');post_id=parts[4] if len(parts)>4 else None
                    post=posts.get(post_id)
                    if method=='GET':
                        if not post or post['status']=='deleted' or not is_owner and (post['visibility']!='public' or post['status']!='published'): reply(status=404)
                        else: reply(material(post,is_owner))
                        return
                    key=route.request.headers.get('idempotency-key');writes.append({'key':key,'body':route.request.post_data_json,'method':method})
                    assert key
                    if key in commands: reply(commands[key]);return
                    body=route.request.post_data_json
                    if not post_id:
                        post_id='post-'+str(len(posts)+1)
                        post={'id':post_id,'contentId':post_id,'version':1,'permissionVersion':1,'kind':body['kind'],'title':body['title'],'body':body['body'],'visibility':body['visibility'],'status':'draft','author':{'id':'owner','name':'Creator'},'mediaSourceContentId':body.get('mediaSourceContentId'),'assetId':None,'createdAt':time.time(),'updatedAt':time.time(),'publishedAt':None,'sharePath':None,'canEdit':True}
                        posts[post_id]=post
                    else:
                        if control['conflict']:
                            control['conflict']=False;post['version']+=1;reply(status=409);return
                        if body['expectedVersion'] != post['version']: reply(status=409);return
                        post['version']+=1
                        if path.endswith('/publish'):post['status']='published';post['publishedAt']=time.time()
                        elif method=='DELETE':post['status']='deleted'
                        else:
                            old=post['visibility'];post.update({key:value for key,value in body.items() if key!='expectedVersion'})
                            if old!=post['visibility']:post['permissionVersion']+=1
                    post['sharePath']='/#/content/'+post_id if post['status']=='published' and post['visibility']=='public' else None
                    commands[key]=material(post,True)
                    if control['fail_create']:
                        control['fail_create']=False;reply(status=503)
                    else:reply(commands[key])
                else: reply({})
            owner.route('**/api/**', lambda route:api(route,True));viewer.route('**/api/**',lambda route:api(route,False))
            for _ in range(40):
                try:owner.goto(base+'#/studio',timeout=1000);break
                except Exception:time.sleep(.1)
            owner.get_by_label('标题',exact=True).fill('音乐测试 First post')
            body='<img src=x onerror="window.__pwned=1">\n音乐感受。'
            owner.get_by_label('正文',exact=True).fill(body)
            owner.get_by_label('可见范围',exact=True).select_option('public')
            owner.get_by_role('button',name='保存',exact=True).click()
            owner.get_by_text('已保存',exact=True).wait_for()
            assert len(posts)==1 and writes[0]['key']==writes[1]['key']
            checks['draft_creation_network_retry_is_idempotent']=True
            owner.screenshot(path=str(ROOT/'doc/ugc-studio-2026-10-08.png'))
            viewer.goto(base+'#/content/post-1');viewer.get_by_text('内容不存在，或当前账号没有查看权限。').wait_for()
            checks['draft_authorization_denies_other_viewer']=True
            owner.get_by_role('button',name='发布',exact=True).click();owner.get_by_text('已发布',exact=True).wait_for()
            viewer.reload();viewer.get_by_role('heading',name='音乐测试 First post').wait_for()
            assert viewer.locator('.body').inner_text()==body and viewer.locator('.body img').count()==0
            assert viewer.evaluate('window.__pwned === undefined')
            assert viewer.locator('video').count()==0
            assert viewer.locator('[aria-label="作者关注"]').count()==0
            checks['published_text_is_plain_text_without_video_or_html_execution']=True
            viewer.get_by_role('button',name='分享',exact=True).click()
            viewer.get_by_role('textbox',name='分享链接').wait_for()
            checks['public_share_reauthorizes_and_exposes_current_link']=True
            viewer.screenshot(path=str(ROOT/'doc/ugc-detail-mobile-2026-10-08.png'))
            owner.get_by_label('正文',exact=True).fill('保留输入，等待冲突处理')
            control['conflict']=True
            owner.get_by_role('button',name='保存',exact=True).click()
            owner.get_by_text('内容已在其他窗口更新。当前输入已保留，请重新加载最新版本再编辑。').wait_for()
            assert owner.get_by_label('正文',exact=True).input_value()=='保留输入，等待冲突处理'
            checks['cas_conflict_preserves_user_input']=True
            owner.on('dialog',lambda dialog:dialog.accept())
            owner.get_by_role('button',name='重新加载',exact=True).click()
            owner.get_by_label('可见范围',exact=True).select_option('private')
            owner.get_by_role('button',name='保存',exact=True).click();owner.get_by_text('已保存',exact=True).wait_for()
            viewer.reload();viewer.get_by_text('内容不存在，或当前账号没有查看权限。').wait_for()
            checks['public_to_private_revokes_detail']=True
            owner.get_by_label('可见范围',exact=True).select_option('public')
            owner.get_by_role('button',name='保存',exact=True).click();owner.get_by_text('已保存',exact=True).wait_for()
            viewer.goto(base+'#/content-search');viewer.get_by_role('combobox').fill('音乐')
            viewer.locator('#content-suggestions button').wait_for();checks['authorized_suggestions_visible']=True
            viewer.get_by_role('combobox').press('ArrowDown');viewer.get_by_role('combobox').press('Enter')
            viewer.get_by_role('heading',name='音乐测试 First post').wait_for()
            checks['keyboard_suggestion_opens_post_detail']=True
            viewer.goto(base+'#/content-search');viewer.get_by_role('combobox').fill('音乐')
            viewer.locator('#content-suggestions button').wait_for()
            control['delay_suggest']=True
            viewer.get_by_role('combobox').fill('First');viewer.wait_for_timeout(400)
            assert control['delayed_suggest']
            viewer.get_by_role('combobox').fill('no-matching-post');viewer.wait_for_timeout(400)
            stale,rows=control['delayed_suggest']
            try:stale.fulfill(content_type='application/json',body=json.dumps({'success':True,'data':{'items':rows}}))
            except Exception:pass
            viewer.wait_for_timeout(100)
            assert viewer.locator('#content-suggestions').count()==0
            checks['stale_suggestions_do_not_cross_query_generation']=True
            viewer.get_by_role('combobox').fill('音乐')
            viewer.get_by_role('button',name='搜索',exact=True).click();viewer.locator('.result-title').wait_for()
            control['delay_search']=True
            viewer.get_by_role('button',name='搜索',exact=True).click()
            viewer.wait_for_timeout(100)
            assert control['delayed']
            posts['post-1']['visibility']='private';posts['post-1']['permissionVersion']+=1
            viewer.evaluate("() => { const channel = new BroadcastChannel('radio-content-changed'); channel.postMessage({invalidate:true}); channel.close() }")
            viewer.wait_for_timeout(200)
            delayed,rows,q=control['delayed']
            try:delayed.fulfill(content_type='application/json',body=json.dumps({'success':True,'data':{'items':rows,'query':q,'hasMore':False}}))
            except Exception:pass
            viewer.wait_for_timeout(300)
            assert viewer.locator('.result-title').count()==0 and viewer.locator('#content-suggestions').count()==0
            checks['privacy_invalidation_prevents_late_search_restoration']=True
            owner.reload();owner.locator('.post-row').first.click()
            owner.get_by_role('button',name='删除内容',exact=True).click()
            owner.get_by_role('button',name='确认删除',exact=True).click();owner.get_by_text('已删除',exact=True).wait_for()
            viewer.goto(base+'#/content/post-1');viewer.get_by_text('内容不存在，或当前账号没有查看权限。').wait_for()
            checks['delete_revokes_post_and_share_entry']=True
            viewer.screenshot(path=str(ROOT/'doc/ugc-search-mobile-2026-10-08.png'))
            assert not errors,errors
            checks['javascript_errors']=errors
            browser.close()
    finally:
        server.terminate();server.wait(timeout=10);log.close()
        (ROOT/'doc/ugc-frontend-browser-2026-10-08.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(checks,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
