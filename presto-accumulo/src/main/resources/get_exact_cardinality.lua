local EMPTY, TERM, OR, AND = 0, 1, 2, 3

local function get_parse_tree(expression)

    local Node = {}
    function Node.new(type, start, _end)
        local node = {}
        setmetatable(node, Node)
        node.__index = node
        node.type = type
        node.start = start
        node._end = _end
        node.children = {}

        function node:add(child)
            self.children[#self.children + 1] = child
        end

        function node:get_term(expression)
            local function char_at(expression, index)
                return expression:sub(index + 1, index + 1)
            end

            if char_at(expression, self.start) == '"' then
                return expression:sub(self.start + 2, self._end - 1)
            else
                return expression:sub(self.start + 1, self._end)
            end
        end

        return node
    end


    local ColumnVisibilityParser = {}
    ColumnVisibilityParser.__index = ColumnVisibilityParser
    function ColumnVisibilityParser.create()
        local obj = {}
        setmetatable(obj, ColumnVisibilityParser)
        obj.index = 0
        obj.parens = 0
        return obj
    end

    function ColumnVisibilityParser:processTerm(start, _end, expr, expression)
        if start ~= _end then
            if expr ~= nil then
                error("expression needs | or &")
            end

            return Node.new(TERM, start, _end)
        end

        if expr == nil then
            error("empty term")
        end

        return expr
    end

    function ColumnVisibilityParser:parse(expression)

        if string.len(expression) <= 0 then return nil end

        local function char_at(expression, index)
            return expression:sub(index + 1, index + 1)
        end

        local result = nil
        local expr = nil
        local wholeTermStart = self.index
        local subtermStart = self.index
        local subtermComplete = false
        local expressionLength = string.len(expression)

        while (self.index <= expressionLength) do
            local char = char_at(expression, self.index)
            self.index = self.index + 1

            if char == '&' then
                expr = self:processTerm(subtermStart, self.index - 1, expr, expression)

                if result ~= nil then
                    if result.type ~= AND then
                        error("cannot mix & and |")
                    end
                else
                    result = Node.new(AND, wholeTermStart, wholeTermStart + 1)
                end

                result:add(expr)
                expr = nil
                subtermStart = self.index
                subtermComplete = false
            elseif char == '|' then
                expr = self:processTerm(subtermStart, self.index - 1, expr, expression)
                if result ~= nil then
                    if result.type ~= OR then
                        error("cannot mix | and &")
                    end
                else
                    result = Node.new(OR, wholeTermStart, wholeTermStart + 1)
                end

                result:add(expr)
                expr = nil
                subtermStart = self.index
                subtermComplete = false
            elseif char == '(' then
                self.parens = self.parens + 1
                if subtermStart ~= self.index - 1 or expr ~= nil then
                    error("expression needs & or |")
                end
                expr = self:parse(expression);
                subtermStart = self.index;
                subtermComplete = false;
            elseif char == ')' then
                self.parens = self.parens - 1
                local child = self:processTerm(subtermStart, self.index - 1, expr, expression)
                if child == nil and result == nil then
                    error("empty expression not allowed")
                end
                if result == nil then
                    return child
                end
                if result.type == child.type then
                    for _, child in ipairs(child.children) do
                        result:add(child)
                    end
                else
                    result:add(child)
                end

                result._end = self.index - 1
                return result
            elseif char == '"' then
                if subtermStart ~= self.index - 1 then
                    error("expression needs & or |")
                end

                while self.index < expressionLength and char_at(expression, self.index) ~= "\"" do
                    if char_at(expression, self.index) == '\\' then
                        self.index = self.index + 1
                        if char_at(expression, self.index) ~= '\\' and char_at(expression, self.index) ~= '"' then
                            error("invalid escaping within quotes")
                        end
                    end
                    self.index = self.index + 1
                end

                if self.index == expressionLength then
                    error("unclosed quote")
                end

                if subtermStart + 1 == self.index then
                    error("empty term")
                end

                self.index = self.index + 1
                subtermComplete = true
            else
                if subtermComplete then
                    error("expression needs & or |")
                end
                -- TODO Validate chars?
            end
        end -- end while loop

        local child = self:processTerm(subtermStart, self.index - 1, expr, expression)

        if result ~= nil then
            result:add(child)
            result._end = self.index
        else
            result = child
        end

        local length = #result.children
        if result.type ~= TERM and #result.children < 2 then
            error("missing term")
        end
        return result
    end


    local obj = {}
    if expression ~= nil and string.len(expression) > 0 then
        local parser = ColumnVisibilityParser.create()
        obj.node = parser:parse(expression)
    else
        obj.node = Node.new(EMPTY, 0, 0)
    end

    return obj.node
end

local function stringify(root, expression, str)
    if root.type == TERM then
        return str .. string.sub(expression, root.start + 1, root._end)
    else
        local sep = ""
        for _, v in pairs(root.children) do
            str = str .. sep
            local parens = v.type ~= TERM and root.type ~= v.type
            if parens then
                str = str .. '('
            end

            str = stringify(v, expression, str)

            if parens then
                str = str .. ')'
            end

            if root.type == AND then
                sep = "&"
            else
                sep = "|"
            end
        end

        return str
    end
end


local function evaluate(auths, expression, root)
    if string.len(expression) == 0 then
        return true
    else
        if root.type == TERM then
            if auths[root:get_term(expression)] then
                return true
            end
            return false

        elseif root.type == AND then
            for k, v in pairs(root.children) do
                if not evaluate(auths, expression, v) then
                    return false
                end
            end
            return true
        elseif root.type == OR then
            for _, v in pairs(root.children) do
                if evaluate(auths, expression, v) then
                    return true
                end
            end
            return false
        else
            error("unknown root type " .. root.type)
        end
    end
end

local function split(str, inSplitPattern)
    local outResults = {}
    local theStart = 1
    local theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart)
    while theSplitStart do
        outResults[string.sub(str, theStart, theSplitStart - 1)] = true
        theStart = theSplitEnd + 1
        theSplitStart, theSplitEnd = string.find(str, inSplitPattern, theStart)
    end
    outResults[string.sub(str, theStart)] = true
    return outResults
end

local function get_visibility(key)
    local i = key:match(".*::()")
    if i then return key:sub(i, key:len()) else return nil end
end

local function get_hash_key_string(schema, table, column_family, visibility)
    return "pacc::cardhs::" .. schema .. "::" .. table .. "::" .. column_family .. "::" .. visibility;
end

local auths = split(ARGV[1], ',')
local value = ARGV[2]

local sum = 0
local keys = redis.call('keys', KEYS[1])
for i, hash_key in ipairs(keys) do
    local visibility = get_visibility(hash_key)
    if visibility:len() == 0 or evaluate(auths, visibility, get_parse_tree(visibility)) then
        local val = redis.call('hget', hash_key, value)
        if val then sum = sum + tonumber(val) end
    end
end

return sum

