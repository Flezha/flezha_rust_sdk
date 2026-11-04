# The controller's image: Flezha's own, with the plugin packages in plugins/.
FROM flezha/flezha:0.1.1
COPY --chown=flezha:flezha plugins/ /flezha/plugins/
ENV FLEZHA_PLUGIN_ROOT=/flezha/plugins
