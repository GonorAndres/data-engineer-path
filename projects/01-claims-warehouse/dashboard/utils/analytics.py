"""PostHog web analytics for the FastAPI claims dashboard.

Every site in the portfolio reports into one PostHog project, so `$host` cannot
separate them -- the same app shows up under its `gonor.me` domain and under the
provider URL it is really deployed on. The `app_id` registered below is the only
reliable separator, which is why the `register()` call matters as much as `init()`.

`api_host` is derived from `location.origin` rather than hardcoded, so the snippet
works unchanged on `data-engineer.gonor.me` and on the `run.app` hostname behind it,
and is same-origin in both cases -- no CORS, and no adblocker dropping requests to a
known analytics domain. The `/ingest` path is served by this app (see `main.py`).

The `phc_...` key is a public write-only project key, safe to commit.
"""

# Stable, kebab-case, and never renamed: it is the join key for every insight.
APP_ID = "claims-dashboard"
CANONICAL_HOST = "data-engineer.gonor.me"
POSTHOG_TOKEN = "phc_DYrSznvPeJuXPHgj2Nw9BIluiGdwkbuSSih3lu6PtmH"

_SNIPPET_TEMPLATE = """
<script>
!function(t,e){var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){function g(t,e){var o=e.split(".");2==o.length&&(t=t[o[0]],e=o[1]),t[e]=function(){t.push([e].concat(Array.prototype.slice.call(arguments,0)))}}(p=t.createElement("script")).type="text/javascript",p.crossOrigin="anonymous",p.async=!0,p.src=s.api_host+"/static/array.js",(r=t.getElementsByTagName("script")[0]).parentNode.insertBefore(p,r);var u=e;for(void 0!==a?u=e[a]=[]:a="posthog",u.people=u.people||[],u.toString=function(t){var e="posthog";return"posthog"!==a&&(e+="."+a),t||(e+=" (stub)"),e},u.people.toString=function(){return u.toString(1)+".people (stub)"},o="init capture register register_once unregister opt_in_capturing opt_out_capturing has_opted_in_capturing has_opted_out_capturing identify alias people.set people.set_once set_config reset get_distinct_id getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags group updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures getActiveMatchingSurveys getSurveys onFeatureFlags onSessionId setPersonProperties".split(" "),n=0;n<o.length;n++)g(u,o[n]);e._i.push([i,s,a])},e.__SV=1)}(document,window.posthog||[]);
var CANONICAL_HOST = '__CANONICAL_HOST__';
posthog.init('__TOKEN__', {
  api_host: location.origin + '/ingest',
  ui_host: 'https://us.posthog.com',
  autocapture: true,
  capture_pageview: 'history_change',
  capture_pageleave: true,
  session_recording: { maskAllInputs: true }
});
posthog.register({
  app_id: '__APP_ID__',
  canonical_host: CANONICAL_HOST,
  deployment_platform: 'cloud-run',
  environment: location.hostname === CANONICAL_HOST ? 'production' : 'preview',
  analytics_schema_version: 1
});
</script>
"""
POSTHOG_SNIPPET = (
    _SNIPPET_TEMPLATE.replace("__CANONICAL_HOST__", CANONICAL_HOST)
    .replace("__TOKEN__", POSTHOG_TOKEN)
    .replace("__APP_ID__", APP_ID)
)
